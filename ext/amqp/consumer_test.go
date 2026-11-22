package amqp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcersing/eventbus"
	"github.com/kcersing/eventbus/ext/amqp/amqpclt"
)

func startConsumer(t *testing.T, c *Consumer) {
	t.Helper()
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(c.Stop)
}

func TestConsumerAcksAfterHandler(t *testing.T) {
	sub := &fakeSubscriber{deliveries: make(chan *amqpclt.Delivery)}
	var ack, nack atomic.Int32
	consumer := newConsumer(sub, eventbus.HandlerFunc(
		func(context.Context, *eventbus.Event) error { return nil }))
	startConsumer(t, consumer)

	sub.deliveries <- newFakeDelivery("consumer-ok", &ack, &nack)

	waitFor(t, 2*time.Second, func() bool { return ack.Load() == 1 })
	if nack.Load() != 0 {
		t.Fatalf("nack = %d, want 0", nack.Load())
	}
}

func TestConsumerRetriesTransientErrors(t *testing.T) {
	sub := &fakeSubscriber{deliveries: make(chan *amqpclt.Delivery)}
	var ack, nack, attempts atomic.Int32
	consumer := newConsumer(sub, eventbus.HandlerFunc(
		func(context.Context, *eventbus.Event) error {
			if attempts.Add(1) == 1 {
				return errors.New("transient")
			}
			return nil
		}),
		WithMaxRetries(3),
		WithRetryBackoff(time.Millisecond),
	)
	startConsumer(t, consumer)

	sub.deliveries <- newFakeDelivery("consumer-retry", &ack, &nack)

	waitFor(t, 2*time.Second, func() bool { return ack.Load() == 1 })
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if nack.Load() != 0 {
		t.Fatalf("nack = %d, want 0", nack.Load())
	}
}

func TestConsumerDeadLettersUnrecoverable(t *testing.T) {
	sub := &fakeSubscriber{deliveries: make(chan *amqpclt.Delivery)}
	var ack, nack, attempts atomic.Int32
	var requeue atomic.Bool
	consumer := newConsumer(sub, eventbus.HandlerFunc(
		func(context.Context, *eventbus.Event) error {
			attempts.Add(1)
			return eventbus.Unrecoverable(errors.New("bad payload"))
		}),
		WithMaxRetries(5),
	)
	startConsumer(t, consumer)

	delivery := &amqpclt.Delivery{
		Message: &amqpclt.Message{CorrelationId: "consumer-bad", Event: "order.created"},
		Ack:     func() error { ack.Add(1); return nil },
		Nack: func(r bool) error {
			requeue.Store(r)
			nack.Add(1)
			return nil
		},
	}
	sub.deliveries <- delivery

	waitFor(t, 2*time.Second, func() bool { return nack.Load() == 1 })
	if ack.Load() != 0 {
		t.Fatalf("ack = %d, want 0", ack.Load())
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1", attempts.Load())
	}
	if requeue.Load() {
		t.Fatal("dead letter must use Nack(requeue=false)")
	}
}

func TestConsumerAfterCommitFailureStillAcks(t *testing.T) {
	sub := &fakeSubscriber{deliveries: make(chan *amqpclt.Delivery)}
	var ack, nack, after atomic.Int32
	consumer := newConsumer(sub, eventbus.HandlerFunc(
		func(context.Context, *eventbus.Event) error { return nil }),
		WithAfterCommit(func(context.Context, *eventbus.Event) error {
			after.Add(1)
			return errors.New("cache down")
		}),
	)
	startConsumer(t, consumer)

	sub.deliveries <- newFakeDelivery("consumer-after", &ack, &nack)

	waitFor(t, 2*time.Second, func() bool { return ack.Load() == 1 })
	if after.Load() != 1 {
		t.Fatalf("after commit calls = %d, want 1", after.Load())
	}
	if nack.Load() != 0 {
		t.Fatalf("nack = %d, want 0", nack.Load())
	}
}

func TestConsumerProcessTimeout(t *testing.T) {
	sub := &fakeSubscriber{deliveries: make(chan *amqpclt.Delivery)}
	var ack, nack, attempts atomic.Int32
	var requeue atomic.Bool
	consumer := newConsumer(sub, eventbus.HandlerFunc(
		func(ctx context.Context, _ *eventbus.Event) error {
			attempts.Add(1)
			<-ctx.Done()
			return ctx.Err()
		}),
		WithProcessTimeout(20*time.Millisecond),
		WithMaxRetries(0),
	)
	startConsumer(t, consumer)

	delivery := &amqpclt.Delivery{
		Message: &amqpclt.Message{CorrelationId: "consumer-timeout", Event: "order.created"},
		Ack:     func() error { ack.Add(1); return nil },
		Nack: func(r bool) error {
			requeue.Store(r)
			nack.Add(1)
			return nil
		},
	}
	sub.deliveries <- delivery

	waitFor(t, 2*time.Second, func() bool { return nack.Load() == 1 })
	if ack.Load() != 0 {
		t.Fatalf("ack = %d, want 0", ack.Load())
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1", attempts.Load())
	}
	if requeue.Load() {
		t.Fatal("exhausted retries must use Nack(requeue=false)")
	}
}

func TestConsumerCustomClassifierDrop(t *testing.T) {
	sub := &fakeSubscriber{deliveries: make(chan *amqpclt.Delivery)}
	var ack, nack, attempts atomic.Int32
	consumer := newConsumer(sub, eventbus.HandlerFunc(
		func(context.Context, *eventbus.Event) error {
			attempts.Add(1)
			return errors.New("ignore")
		}),
		WithMaxRetries(3),
		WithRetryClassifier(func(context.Context, *eventbus.Event, error) eventbus.RetryDecision {
			return eventbus.Drop
		}),
	)
	startConsumer(t, consumer)

	sub.deliveries <- newFakeDelivery("consumer-drop", &ack, &nack)

	waitFor(t, 2*time.Second, func() bool { return ack.Load() == 1 })
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1", attempts.Load())
	}
	if nack.Load() != 0 {
		t.Fatalf("nack = %d, want 0", nack.Load())
	}
}
