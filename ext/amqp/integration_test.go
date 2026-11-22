package amqp

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/kcersing/eventbus"
	"github.com/kcersing/eventbus/ext/amqp/amqpclt"
	amqp "github.com/rabbitmq/amqp091-go"
)

func integrationConn(t *testing.T) *amqp.Connection {
	t.Helper()
	url := os.Getenv("AMQP_URL")
	if url == "" {
		t.Skip("AMQP_URL not set; skipping RabbitMQ integration test")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func uniqueBase() string {
	return fmt.Sprintf("eventbus.it.%d", time.Now().UnixNano())
}

func cleanupResources(t *testing.T, conn *amqp.Connection, queues, exchanges []string) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Logf("cleanup: open channel: %v", err)
		return
	}
	defer ch.Close()

	for _, q := range queues {
		if _, err := ch.QueueDelete(q, false, false, false); err != nil {
			t.Logf("cleanup: delete queue %s: %v", q, err)
		}
	}
	for _, ex := range exchanges {
		if err := ch.ExchangeDelete(ex, false, false); err != nil {
			t.Logf("cleanup: delete exchange %s: %v", ex, err)
		}
	}
}

// Consumer 的真实链路：Handler 成功后 Ack，队列中不再残留消息。
func TestIntegrationConsumerReceivesAndAcks(t *testing.T) {
	conn := integrationConn(t)
	base := uniqueBase()
	exchange, queue := base+".exchange", base+".queue"
	t.Cleanup(func() { cleanupResources(t, conn, []string{queue}, []string{exchange}) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 先声明队列与绑定，避免发布时队列尚未创建导致 fanout 消息丢失。
	pre, err := amqpclt.NewSubscribe(conn, exchange, amqpclt.WithQueueName(queue))
	if err != nil {
		t.Fatalf("predeclare NewSubscribe: %v", err)
	}
	_, cleanupPre, err := pre.SubscribeManual(ctx)
	if err != nil {
		t.Fatalf("predeclare SubscribeManual: %v", err)
	}
	cleanupPre()

	got := make(chan *eventbus.Event, 1)
	sub, err := amqpclt.NewSubscribe(conn, exchange,
		amqpclt.WithQueueName(queue),
		amqpclt.WithQoS(1),
	)
	if err != nil {
		t.Fatalf("NewSubscribe: %v", err)
	}
	consumer := NewConsumer(sub, eventbus.HandlerFunc(
		func(_ context.Context, e *eventbus.Event) error {
			got <- e
			return nil
		}), WithMaxRetries(0))
	if err := consumer.Start(ctx); err != nil {
		t.Fatalf("Consumer.Start: %v", err)
	}
	defer consumer.Stop()

	pub, err := amqpclt.NewPublisher(conn, exchange)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish(ctx, "", "corr-consumer", amqpclt.Message{
		Event:   "order.created",
		Payload: map[string]any{"id": 9},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case e := <-got:
		if e.Topic != "order.created" {
			t.Fatalf("event topic = %q, want order.created", e.Topic)
		}
		if e.Id != "corr-consumer" {
			t.Fatalf("event id = %q, want corr-consumer", e.Id)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for handler")
	}

	consumer.Stop() // 等待在途 Handler 退出并完成 Ack

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer ch.Close()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclarePassive: %v", err)
	}
	if q.Messages != 0 {
		t.Fatalf("queue still has %d messages after ack", q.Messages)
	}
}

// recordingRecorder 记录 Listener 投递回执。
type recordingRecorder struct {
	mu       sync.Mutex
	receipts []eventbus.DeliveryReceipt
	recorded chan struct{}
}

func (r *recordingRecorder) Record(_ context.Context, receipts []eventbus.DeliveryReceipt) error {
	r.mu.Lock()
	r.receipts = append(r.receipts, receipts...)
	r.mu.Unlock()
	select {
	case r.recorded <- struct{}{}:
	default:
	}
	return nil
}

func (r *recordingRecorder) snapshot() []eventbus.DeliveryReceipt {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]eventbus.DeliveryReceipt(nil), r.receipts...)
}

// Listener 真实链路：MQ → 总线 → 记录回执 → Ack。
func TestIntegrationListenerPublishesAndRecords(t *testing.T) {
	conn := integrationConn(t)
	base := uniqueBase()
	exchange, queue := base+".exchange", base+".queue"
	t.Cleanup(func() { cleanupResources(t, conn, []string{queue}, []string{exchange}) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 先声明队列与绑定，避免 Listener 异步订阅前发布的消息丢失。
	pre, err := amqpclt.NewSubscribe(conn, exchange, amqpclt.WithQueueName(queue))
	if err != nil {
		t.Fatalf("predeclare NewSubscribe: %v", err)
	}
	_, cleanupPre, err := pre.SubscribeManual(ctx)
	if err != nil {
		t.Fatalf("predeclare SubscribeManual: %v", err)
	}
	cleanupPre()

	bus := eventbus.New()
	defer bus.Close()
	handled := make(chan *eventbus.Event, 1)
	sub := bus.SubscribeAsync(ctx, "order.created", eventbus.HandlerFunc(
		func(_ context.Context, e *eventbus.Event) error {
			handled <- e
			return nil
		}), 1)
	defer sub.Unsubscribe()

	recorder := &recordingRecorder{recorded: make(chan struct{}, 1)}
	subscriber, err := amqpclt.NewSubscribe(conn, exchange,
		amqpclt.WithQueueName(queue),
		amqpclt.WithQoS(1),
	)
	if err != nil {
		t.Fatalf("NewSubscribe: %v", err)
	}
	listener := NewListener(bus, subscriber, WithDeliveryRecorder(recorder))
	if err := listener.Start(ctx); err != nil {
		t.Fatalf("Listener.Start: %v", err)
	}
	defer listener.Stop()

	pub, err := amqpclt.NewPublisher(conn, exchange)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish(ctx, "", "corr-listener", amqpclt.Message{
		Event:   "order.created",
		Payload: map[string]any{"id": 11},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case e := <-handled:
		if e.Topic != "order.created" {
			t.Fatalf("event topic = %q, want order.created", e.Topic)
		}
		if e.Id != "corr-listener" {
			t.Fatalf("event id = %q, want corr-listener", e.Id)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for bus handler")
	}

	select {
	case <-recorder.recorded:
	case <-ctx.Done():
		t.Fatal("timed out waiting for receipt record")
	}
	receipts := recorder.snapshot()
	if len(receipts) != 1 {
		t.Fatalf("receipts = %d, want 1", len(receipts))
	}
	if receipts[0].EventID != "corr-listener" {
		t.Fatalf("receipt event id = %q, want corr-listener", receipts[0].EventID)
	}
	if receipts[0].Status != eventbus.DeliveryQueued {
		t.Fatalf("receipt status = %v, want queued", receipts[0].Status)
	}

	listener.Stop() // 等待回执记录后完成 Ack

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer ch.Close()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclarePassive: %v", err)
	}
	if q.Messages != 0 {
		t.Fatalf("queue still has %d messages after ack", q.Messages)
	}
}
