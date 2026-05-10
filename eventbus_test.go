package eventbus_test

import (
	"context"
	"errors"
	"eventbus"
	"sync/atomic"
	"testing"
	"time"
)

// TestNewEvent verifies event creation
func TestNewEvent(t *testing.T) {
	ev := eventbus.NewEvent("test.topic", "hello")
	if ev.Topic != "test.topic" {
		t.Errorf("unexpected topic: %s", ev.Topic)
	}
	if ev.Id == "" {
		t.Error("event id empty")
	}
	if ev.Timestamp.IsZero() {
		t.Error("timestamp is zero")
	}
}

// TestPublishSubscribe basic pub/sync sub
func TestPublishSubscribe(t *testing.T) {
	bus := eventbus.NewEventBus()
	defer bus.Close()
	ctx := context.Background()
	ch := bus.Subscribe(ctx, "test.topic")
	var received atomic.Int32
	go func() {
		for ev := range ch {
			if ev.Topic == "test.topic" {
				received.Add(1)
			}
		}
	}()
	bus.PublishByTopic(ctx, "test.topic", "data")
	time.Sleep(100 * time.Millisecond)
	if received.Load() != 1 {
		t.Errorf("expected 1, got %d", received.Load())
	}
}

// TestSubscribeAsync async subscription
func TestSubscribeAsync(t *testing.T) {
	bus := eventbus.NewEventBus()
	defer bus.Close()
	var count atomic.Int32
	handler := eventbus.EventHandlerFunc(func(ctx context.Context, ev *eventbus.Event) error { count.Add(1); return nil })
	sub := bus.SubscribeAsync(context.Background(), "async.topic", handler, 2)
	defer sub.Unsubscribe()
	for i := 0; i < 10; i++ {
		bus.PublishByTopic(context.Background(), "async.topic", i)
	}
	time.Sleep(100 * time.Millisecond)
	if count.Load() != 10 {
		t.Errorf("expected 10, got %d", count.Load())
	}
}

// TestWildcard wildcard subscription
func TestWildcard(t *testing.T) {
	bus := eventbus.NewEventBus()
	defer bus.Close()
	var count atomic.Int32
	bus.SubscribeAsync(context.Background(), "order.*", eventbus.EventHandlerFunc(func(ctx context.Context, ev *eventbus.Event) error { count.Add(1); return nil }), 1)
	bus.PublishByTopic(context.Background(), "order.created", nil)
	bus.PublishByTopic(context.Background(), "order.paid", nil)
	time.Sleep(100 * time.Millisecond)
	if count.Load() != 2 {
		t.Errorf("expected 2, got %d", count.Load())
	}
}

// TestMiddleware middleware chain ordering
func TestMiddleware(t *testing.T) {
	bus := eventbus.NewEventBus()
	defer bus.Close()
	var order []string
	bus.Use(func(next eventbus.Handler) eventbus.Handler {
		return eventbus.EventHandlerFunc(func(ctx context.Context, ev *eventbus.Event) error {
			order = append(order, "before")
			err := next.Handle(ctx, ev)
			order = append(order, "after")
			return err
		})
	})
	ch := bus.Subscribe(context.Background(), "mw.topic")
	bus.PublishByTopic(context.Background(), "mw.topic", nil)
	select {
	case ev := <-ch:
		order = append(order, "handler")
		_ = ev
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
	if len(order) != 3 || order[0] != "before" || order[1] != "after" {
		t.Errorf("expected [before after handler], got %v", order)
	}
}

// TestPanicRecovery handler panic should not crash
func TestPanicRecovery(t *testing.T) {
	bus := eventbus.NewEventBus()
	defer bus.Close()
	var reached atomic.Int32
	bus.SubscribeAsync(context.Background(), "panic.topic", eventbus.EventHandlerFunc(func(ctx context.Context, ev *eventbus.Event) error { panic("intentional") }), 1)
	bus.SubscribeAsync(context.Background(), "panic.topic", eventbus.EventHandlerFunc(func(ctx context.Context, ev *eventbus.Event) error { reached.Add(1); return nil }), 1)
	bus.PublishByTopic(context.Background(), "panic.topic", nil)
	time.Sleep(100 * time.Millisecond)
	if reached.Load() != 1 {
		t.Errorf("expected second handler, got %d", reached.Load())
	}
}

// TestEventPublisher publisher scopes
func TestEventPublisher(t *testing.T) {
	bus := eventbus.NewEventBus()
	defer bus.Close()
	pub := eventbus.NewEventPublisher(bus, nil)
	var count atomic.Int32
	bus.SubscribeAsync(context.Background(), "pub.topic", eventbus.EventHandlerFunc(func(ctx context.Context, ev *eventbus.Event) error { count.Add(1); return nil }), 1)
	_ = pub.Local(context.Background(), "pub.topic", "hello")
	time.Sleep(50 * time.Millisecond)
	if count.Load() != 1 {
		t.Errorf("expected 1, got %d", count.Load())
	}
	pub.Close()
}

// TestConsumerRegistry registry lifecycle
func TestConsumerRegistry(t *testing.T) {
	bus := eventbus.NewEventBus()
	defer bus.Close()
	reg := eventbus.NewConsumerRegistry()
	var count atomic.Int32
	_ = reg.RegisterHandler("h1", eventbus.EventHandlerFunc(func(ctx context.Context, ev *eventbus.Event) error { count.Add(1); return nil }))
	_ = reg.RegisterConsumer("reg.topic", "h1", 1)
	_ = reg.StartAll(context.Background(), bus)
	bus.PublishByTopic(context.Background(), "reg.topic", nil)
	time.Sleep(100 * time.Millisecond)
	if count.Load() != 1 {
		t.Errorf("expected 1, got %d", count.Load())
	}
	_ = reg.Shutdown(context.Background())
}

// TestPoolRetry retry + dead letter
func TestPoolRetry(t *testing.T) {
	bus := eventbus.NewEventBus()
	defer bus.Close()
	var attempts atomic.Int32
	var deadLettered atomic.Int32
	sub := bus.SubscribeWithPool(context.Background(), "retry.topic",
		eventbus.EventHandlerFunc(func(ctx context.Context, ev *eventbus.Event) error {
			attempts.Add(1)
			return errors.New("forced error")
		}), 1,
		eventbus.WithMaxRetries(2),
		eventbus.WithRetryBackoff(10*time.Millisecond),
		eventbus.WithDeadLetterFunc(func(ev *eventbus.Event) { deadLettered.Add(1) }),
	)
	defer sub.Unsubscribe()
	bus.PublishByTopic(context.Background(), "retry.topic", nil)
	time.Sleep(200 * time.Millisecond)
	if attempts.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts.Load())
	}
	if deadLettered.Load() != 1 {
		t.Errorf("expected dead letter, got %d", deadLettered.Load())
	}
}

// TestWrapTyped generic handler
func TestWrapTyped(t *testing.T) {
	bus := eventbus.NewEventBus()
	defer bus.Close()
	var result string
	handler := eventbus.WrapTyped(func(ctx context.Context, payload string, ev *eventbus.Event) error { result = payload; return nil })
	bus.SubscribeAsync(context.Background(), "typed.topic", handler, 1)
	bus.PublishByTopic(context.Background(), "typed.topic", "hello typed")
	time.Sleep(100 * time.Millisecond)
	if result != "hello typed" {
		t.Errorf("expected 'hello typed', got '%s'", result)
	}
}

// TestDispatchNoSubscribers no crash without subscribers
func TestDispatchNoSubscribers(t *testing.T) {
	bus := eventbus.NewEventBus()
	defer bus.Close()
	bus.PublishByTopic(context.Background(), "no.subs", "data")
}

// TestUnsubscribeAndClose unsubscribe stops delivery
func TestUnsubscribeAndClose(t *testing.T) {
	bus := eventbus.NewEventBus()
	var count atomic.Int32
	sub := bus.SubscribeAsync(context.Background(), "unsub.topic", eventbus.EventHandlerFunc(func(ctx context.Context, ev *eventbus.Event) error { count.Add(1); return nil }), 1)
	bus.PublishByTopic(context.Background(), "unsub.topic", nil)
	time.Sleep(50 * time.Millisecond)
	if count.Load() != 1 {
		t.Errorf("expected 1, got %d", count.Load())
	}
	sub.Unsubscribe()
	bus.PublishByTopic(context.Background(), "unsub.topic", nil)
	time.Sleep(50 * time.Millisecond)
	if count.Load() != 1 {
		t.Errorf("still got events after unsub: %d", count.Load())
	}
	bus.Close()
}
