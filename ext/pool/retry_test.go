package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcersing/eventbus"
)

func TestRetryClassifierUnrecoverable(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()
	ctx := context.Background()

	var attempts atomic.Int32
	deadLettered := make(chan struct{}, 1)
	sub := SubscribeWithPool(ctx, bus, "classify.dead", eventbus.HandlerFunc(
		func(context.Context, *eventbus.Event) error {
			attempts.Add(1)
			return eventbus.Unrecoverable(errors.New("bad payload"))
		}), 1,
		WithMaxRetries(3),
		WithRetryBackoff(time.Millisecond),
		WithDeadLetterFunc(func(*eventbus.Event) {
			select {
			case deadLettered <- struct{}{}:
			default:
			}
		}),
	)
	defer sub.Unsubscribe()

	bus.PublishTopic(ctx, "classify.dead", nil)

	select {
	case <-deadLettered:
	case <-time.After(2 * time.Second):
		t.Fatal("unrecoverable event was not dead-lettered")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1 (no retry for unrecoverable)", got)
	}
}

func TestRetryClassifierCustomDrop(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()
	ctx := context.Background()

	var attempts atomic.Int32
	deadLettered := atomic.Bool{}
	sub := SubscribeWithPool(ctx, bus, "classify.drop", eventbus.HandlerFunc(
		func(context.Context, *eventbus.Event) error {
			attempts.Add(1)
			return errors.New("ignore me")
		}), 1,
		WithMaxRetries(3),
		WithRetryBackoff(time.Millisecond),
		WithRetryClassifier(func(context.Context, *eventbus.Event, error) eventbus.RetryDecision {
			return eventbus.Drop
		}),
		WithDeadLetterFunc(func(*eventbus.Event) { deadLettered.Store(true) }),
	)
	defer sub.Unsubscribe()

	bus.PublishTopic(ctx, "classify.drop", nil)

	waitFor(t, 2*time.Second, "handler called", func() bool { return attempts.Load() == 1 })
	time.Sleep(50 * time.Millisecond)
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1 for Drop", attempts.Load())
	}
	if deadLettered.Load() {
		t.Fatal("Drop must not call DeadLetterFunc")
	}
}
