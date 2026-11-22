package eventbus

import (
	"context"
	"testing"
	"time"
)

func TestMatchTopicSingleSegment(t *testing.T) {
	cases := []struct {
		pattern string
		topic   string
		want    bool
	}{
		{"order.*", "order.created", true},
		{"order.*", "order.paid", true},
		{"order.*", "order.a.b", false},
		{"order.*", "order", false},
		{"order.*", "user.created", false},
		{"*", "a", true},
		{"*", "a.b", false},
		{"order.*.created", "order.paid.created", true},
		{"order.*.created", "order.paid", false},
		{"order.*.created", "order.a.b.created", false},
		{"order.?", "order.a", true},
		{"order.?", "order.ab", false},
		{"order.?", "order.a.b", false},
		{"order.[ab]", "order.a", true},
		{"order.[ab]", "order.c", false},
		{"order.[ab]", "order.ab", false},
		{"order.[", "order.[", false},
	}
	for _, c := range cases {
		if got := matchTopic(c.pattern, c.topic); got != c.want {
			t.Errorf("matchTopic(%q, %q) = %v, want %v", c.pattern, c.topic, got, c.want)
		}
	}
}

func TestIsWildcardPattern(t *testing.T) {
	cases := []struct {
		pattern string
		want    bool
	}{
		{"order.*", true},
		{"order.?", true},
		{"order.[ab]", true},
		{"order.created", false},
		{"order.[", false},
	}
	for _, c := range cases {
		if got := isWildcardPattern(c.pattern); got != c.want {
			t.Errorf("isWildcardPattern(%q) = %v, want %v", c.pattern, got, c.want)
		}
	}
}

func TestWildcardDoesNotCrossDots(t *testing.T) {
	bus := New()
	defer bus.Close()
	ctx := context.Background()

	ch := bus.Subscribe(ctx, "order.*")
	bus.PublishTopic(ctx, "order.created", "created")
	bus.PublishTopic(ctx, "order.paid", "paid")
	bus.PublishTopic(ctx, "order.a.b", "nested")

	if got := receivePayload(t, ch); got != "created" {
		t.Fatalf("first payload = %v, want created", got)
	}
	if got := receivePayload(t, ch); got != "paid" {
		t.Fatalf("second payload = %v, want paid", got)
	}
	ensureNoEvent(t, ch)
}

func TestWildcardQuestionAndClass(t *testing.T) {
	t.Run("question", func(t *testing.T) {
		bus := New()
		defer bus.Close()
		ctx := context.Background()

		ch := bus.Subscribe(ctx, "order.?")
		bus.PublishTopic(ctx, "order.a", "a")
		bus.PublishTopic(ctx, "order.ab", "ab")
		bus.PublishTopic(ctx, "order.a.b", "nested")

		if got := receivePayload(t, ch); got != "a" {
			t.Fatalf("payload = %v, want a", got)
		}
		ensureNoEvent(t, ch)
	})

	t.Run("class", func(t *testing.T) {
		bus := New()
		defer bus.Close()
		ctx := context.Background()

		ch := bus.Subscribe(ctx, "order.[ab]")
		bus.PublishTopic(ctx, "order.a", "a")
		bus.PublishTopic(ctx, "order.b", "b")
		bus.PublishTopic(ctx, "order.c", "c")
		bus.PublishTopic(ctx, "order.ab", "ab")

		if got := receivePayload(t, ch); got != "a" {
			t.Fatalf("first payload = %v, want a", got)
		}
		if got := receivePayload(t, ch); got != "b" {
			t.Fatalf("second payload = %v, want b", got)
		}
		ensureNoEvent(t, ch)
	})
}

func TestWildcardSegmentCount(t *testing.T) {
	bus := New()
	defer bus.Close()
	ctx := context.Background()

	ch := bus.Subscribe(ctx, "order.*.created")
	bus.PublishTopic(ctx, "order.paid.created", "ok")
	bus.PublishTopic(ctx, "order.paid", "short")
	bus.PublishTopic(ctx, "order.a.b.created", "long")

	if got := receivePayload(t, ch); got != "ok" {
		t.Fatalf("payload = %v, want ok", got)
	}
	ensureNoEvent(t, ch)
}

func TestRemoveWildcardPattern(t *testing.T) {
	bus := New()
	defer bus.Close()
	ctx := context.Background()

	ch := bus.Subscribe(ctx, "order.?")
	bus.RemoveChannel("order.?", ch)
	bus.PublishTopic(ctx, "order.a", "a")
	ensureNoEvent(t, ch)
}

func TestInvalidWildcardPatternFallsBackToExact(t *testing.T) {
	bus := New()
	defer bus.Close()
	ctx := context.Background()

	// 未闭合的 '[' 不是有效 pattern，按精确主题处理，保持旧行为。
	ch := bus.Subscribe(ctx, "order.[")
	bus.PublishTopic(ctx, "order.[", "exact")

	if got := receivePayload(t, ch); got != "exact" {
		t.Fatalf("payload = %v, want exact", got)
	}
}

func receivePayload(t *testing.T, ch EventChan) any {
	t.Helper()
	select {
	case e := <-ch:
		return e.Payload
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
		return nil
	}
}

func ensureNoEvent(t *testing.T, ch EventChan) {
	t.Helper()
	select {
	case e := <-ch:
		t.Fatalf("unexpected event: topic=%s payload=%v", e.Topic, e.Payload)
	case <-time.After(100 * time.Millisecond):
	}
}
