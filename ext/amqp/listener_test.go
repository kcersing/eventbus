package amqp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcersing/eventbus"
	"github.com/kcersing/eventbus/ext/amqp/amqpclt"
)

type fakeSubscriber struct {
	deliveries chan *amqpclt.Delivery
}

func (f *fakeSubscriber) SubscribeManual(context.Context) (<-chan *amqpclt.Delivery, func(), error) {
	return f.deliveries, func() {}, nil
}

type fakeRecorder struct {
	mu       sync.Mutex
	batches  [][]eventbus.DeliveryReceipt
	err      error
	recorded atomic.Bool
}

func (f *fakeRecorder) Record(_ context.Context, receipts []eventbus.DeliveryReceipt) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := append([]eventbus.DeliveryReceipt(nil), receipts...)
	f.batches = append(f.batches, cp)
	f.recorded.Store(true)
	return f.err
}

func (f *fakeRecorder) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

func (f *fakeRecorder) first() []eventbus.DeliveryReceipt {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.batches) == 0 {
		return nil
	}
	return f.batches[0]
}

func newFakeDelivery(id string, ack, nack *atomic.Int32) *amqpclt.Delivery {
	return &amqpclt.Delivery{
		Message: &amqpclt.Message{
			CorrelationId: id,
			Event:         "order.created",
			Payload:       "payload",
			Timestamp:     time.Now(),
		},
		Ack: func() error {
			ack.Add(1)
			return nil
		},
		Nack: func(bool) error {
			nack.Add(1)
			return nil
		},
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met before timeout")
	}
}

func TestListenerRecordsDroppedButAcks(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()

	// 把订阅者通道填满，使下一条投递失败。
	ctx := context.Background()
	ch := bus.Subscribe(ctx, "order.created")
	for i := 0; i < cap(ch); i++ {
		bus.Publish(ctx, eventbus.NewEvent("order.created", i))
	}

	sub := &fakeSubscriber{deliveries: make(chan *amqpclt.Delivery)}
	recorder := &fakeRecorder{}
	listener := newListener(bus, sub, WithDeliveryRecorder(recorder))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := listener.Start(runCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()

	var ack, nack atomic.Int32
	sub.deliveries <- newFakeDelivery("event-dropped", &ack, &nack)

	waitFor(t, 2*time.Second, func() bool { return ack.Load() == 1 })
	if nack.Load() != 0 {
		t.Fatalf("nack = %d, want 0", nack.Load())
	}
	receipts := recorder.first()
	if len(receipts) != 1 {
		t.Fatalf("receipts = %d, want 1", len(receipts))
	}
	if receipts[0].Status != eventbus.DeliveryDropped {
		t.Fatalf("Status = %s, want dropped", receipts[0].Status)
	}
}

func TestListenerAcksAfterRecord(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()
	bus.Subscribe(context.Background(), "order.created") // 产生一条投递回执

	sub := &fakeSubscriber{deliveries: make(chan *amqpclt.Delivery)}
	recorder := &fakeRecorder{}
	listener := newListener(bus, sub, WithDeliveryRecorder(recorder))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()

	var ack, nack atomic.Int32
	sub.deliveries <- newFakeDelivery("event-1", &ack, &nack)

	waitFor(t, 2*time.Second, func() bool { return ack.Load() == 1 })
	if nack.Load() != 0 {
		t.Fatalf("nack = %d, want 0", nack.Load())
	}
	if recorder.count() != 1 {
		t.Fatalf("record batches = %d, want 1", recorder.count())
	}
	receipts := recorder.first()
	if len(receipts) != 1 {
		t.Fatalf("receipts = %d, want 1", len(receipts))
	}
	if receipts[0].EventID != "event-1" {
		t.Fatalf("EventID = %q, want event-1", receipts[0].EventID)
	}
	if receipts[0].Status != eventbus.DeliveryQueued {
		t.Fatalf("Status = %s, want queued", receipts[0].Status)
	}
}

func TestListenerAckHappensAfterRecord(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()
	bus.Subscribe(context.Background(), "order.created")

	sub := &fakeSubscriber{deliveries: make(chan *amqpclt.Delivery)}
	recorder := &fakeRecorder{}
	listener := newListener(bus, sub, WithDeliveryRecorder(recorder))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()

	var ack, nack, ackBeforeRecord atomic.Int32
	delivery := &amqpclt.Delivery{
		Message: &amqpclt.Message{CorrelationId: "event-order", Event: "order.created"},
		Ack: func() error {
			if !recorder.recorded.Load() {
				ackBeforeRecord.Add(1)
			}
			ack.Add(1)
			return nil
		},
		Nack: func(bool) error {
			nack.Add(1)
			return nil
		},
	}
	sub.deliveries <- delivery

	waitFor(t, 2*time.Second, func() bool { return ack.Load() == 1 })
	if ackBeforeRecord.Load() != 0 {
		t.Fatal("Ack happened before delivery receipts were recorded")
	}
	if nack.Load() != 0 {
		t.Fatalf("nack = %d, want 0", nack.Load())
	}
}

func TestListenerDoesNotAckWhenRecordFails(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()
	bus.Subscribe(context.Background(), "order.created")

	sub := &fakeSubscriber{deliveries: make(chan *amqpclt.Delivery)}
	recorder := &fakeRecorder{err: errors.New("db down")}
	listener := newListener(bus, sub, WithDeliveryRecorder(recorder))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()

	var ack, nack atomic.Int32
	sub.deliveries <- newFakeDelivery("event-2", &ack, &nack)

	waitFor(t, 2*time.Second, func() bool { return nack.Load() == 1 })
	if ack.Load() != 0 {
		t.Fatalf("ack = %d, want 0 when record fails", ack.Load())
	}
}

func TestListenerNacksWhenPublishFails(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()
	bus.Use(func(next eventbus.Handler) eventbus.Handler {
		return eventbus.HandlerFunc(func(context.Context, *eventbus.Event) error {
			return errors.New("middleware rejected")
		})
	})

	sub := &fakeSubscriber{deliveries: make(chan *amqpclt.Delivery)}
	recorder := &fakeRecorder{}
	listener := newListener(bus, sub, WithDeliveryRecorder(recorder))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()

	var ack, nack atomic.Int32
	sub.deliveries <- newFakeDelivery("event-3", &ack, &nack)

	waitFor(t, 2*time.Second, func() bool { return nack.Load() == 1 })
	if ack.Load() != 0 {
		t.Fatalf("ack = %d, want 0 when publish fails", ack.Load())
	}
	if recorder.count() != 0 {
		t.Fatalf("record batches = %d, want 0", recorder.count())
	}
}

func TestListenerAcksWithoutRecorder(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()
	bus.Subscribe(context.Background(), "order.created")

	sub := &fakeSubscriber{deliveries: make(chan *amqpclt.Delivery)}
	listener := newListener(bus, sub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer listener.Stop()

	var ack, nack atomic.Int32
	sub.deliveries <- newFakeDelivery("event-4", &ack, &nack)

	waitFor(t, 2*time.Second, func() bool { return ack.Load() == 1 })
	if nack.Load() != 0 {
		t.Fatalf("nack = %d, want 0", nack.Load())
	}
}

// 未调用 Start 时 Stop 应安全返回，不应 panic。
func TestListenerStopWithoutStart(t *testing.T) {
	listener := newListener(nil, nil)
	listener.Stop()
}

// Stop 可重复调用。
func TestListenerStopIsIdempotent(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()

	listener := newListener(bus, &fakeSubscriber{deliveries: make(chan *amqpclt.Delivery)})
	if err := listener.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	listener.Stop()
	listener.Stop()
}
