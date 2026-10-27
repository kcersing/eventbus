package eventbus

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

//- TestPublishSubscribe / TestPublishTopic — 基本发布订阅
//- TestPublishNoSubscriber — 无订阅者不 panic
//- TestSubscribeAsync — 异步订阅 + 多 worker
//- TestWildcardSubscribe / TestWildcardAndExact — 通配符匹配，精确+通配同时命中
//- TestMiddleware — 洋葱模型执行顺序
//- TestMiddlewareErrorAbort — 中间件拒绝时订阅者不触发
//- TestChannelFullDrop — 通道满时丢弃事件
//- TestClose / TestCloseClosesRawChannels — 优雅关闭、幂等性、通道关闭
//- TestConcurrentPublishSubscribe — 50 goroutine 并发发布
//- TestConcurrentSubscribeUnsubscribe — 并发订阅/取消竞态安全
//- TestSubscribeAsyncContextCancel — ctx 取消自动清理
//- TestHandlerPanic — handler panic 恢复后总线继续工作
//- TestRemoveChannel — 移除通道后不再接收事件
// ── Publish / Subscribe ──────────────────────────────────────────────────────

func TestPublishSubscribe(t *testing.T) {
	bus := New()
	ctx := context.Background()

	ch := bus.Subscribe(ctx, "order.created")

	bus.PublishTopic(ctx, "order.created", "payload-1")

	select {
	case e := <-ch:
		if e.Topic != "order.created" {
			t.Errorf("topic = %q, want %q", e.Topic, "order.created")
		}
		if e.Payload != "payload-1" {
			t.Errorf("payload = %v, want %q", e.Payload, "payload-1")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestPublishNoSubscriber(t *testing.T) {
	bus := New()
	// No subscribers — must not panic
	bus.PublishTopic(context.Background(), "no.subscriber", nil)
}

func TestSubscribeAsync(t *testing.T) {
	bus := New()
	defer bus.Close()

	ctx := context.Background()
	var count atomic.Int32

	bus.SubscribeAsync(ctx, "test.topic", HandlerFunc(func(ctx context.Context, e *Event) error {
		count.Add(1)
		return nil
	}), 2)

	// 发布事件，等待异步处理
	for range 10 {
		bus.PublishTopic(ctx, "test.topic", nil)
	}
	time.Sleep(100 * time.Millisecond)

	if n := count.Load(); n != 10 {
		t.Errorf("handler called %d times, want 10", n)
	}
}

// ── Wildcard ─────────────────────────────────────────────────────────────────

func TestWildcardSubscribe(t *testing.T) {
	bus := New()
	ctx := context.Background()

	ch := bus.Subscribe(ctx, "order.*")

	bus.PublishTopic(ctx, "order.created", 1)
	bus.PublishTopic(ctx, "order.paid", 2)
	bus.PublishTopic(ctx, "user.created", 3) // 不应匹配

	received := 0
	timeout := time.After(200 * time.Millisecond)
loop:
	for received < 2 {
		select {
		case <-ch:
			received++
		case <-timeout:
			break loop
		}
	}
	if received != 2 {
		t.Errorf("received %d events via wildcard, want 2", received)
	}
}

func TestWildcardAndExact(t *testing.T) {
	bus := New()
	ctx := context.Background()

	exactCh := bus.Subscribe(ctx, "order.created")
	wildCh := bus.Subscribe(ctx, "order.*")

	bus.PublishTopic(ctx, "order.created", "both")

	// 两个订阅者都应该收到
	select {
	case <-exactCh:
	default:
		t.Error("exact subscriber did not receive event")
	}
	select {
	case <-wildCh:
	default:
		t.Error("wildcard subscriber did not receive event")
	}
}

// ── Middleware ───────────────────────────────────────────────────────────────

func TestMiddleware(t *testing.T) {
	bus := New()
	ctx := context.Background()

	var order []string

	bus.Use(func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, e *Event) error {
			order = append(order, "mw1-before")
			err := next.Handle(ctx, e)
			order = append(order, "mw1-after")
			return err
		})
	})
	bus.Use(func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, e *Event) error {
			order = append(order, "mw2-before")
			err := next.Handle(ctx, e)
			order = append(order, "mw2-after")
			return err
		})
	})

	ch := bus.Subscribe(ctx, "test")
	bus.PublishTopic(ctx, "test", nil)
	<-ch

	// 洋葱模型：mw1 先入栈，后出栈
	expected := []string{"mw1-before", "mw2-before", "mw2-after", "mw1-after"}
	if len(order) != len(expected) {
		t.Fatalf("order length = %d, want %d: %v", len(order), len(expected), order)
	}
	for i := range expected {
		if order[i] != expected[i] {
			t.Errorf("order[%d] = %q, want %q", i, order[i], expected[i])
		}
	}
}

func TestMiddlewareErrorAbort(t *testing.T) {
	bus := New()
	ctx := context.Background()

	stopErr := errors.New("stop")
	bus.Use(func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, e *Event) error {
			return stopErr
		})
	})

	ch := bus.Subscribe(ctx, "test")
	bus.PublishTopic(ctx, "test", nil)

	// 中间件返回错误后不会调用 dispatch，所以订阅者收不到事件
	select {
	case <-ch:
		t.Error("subscriber should not receive event when middleware aborts")
	case <-time.After(100 * time.Millisecond):
	}
}

// ── Close ────────────────────────────────────────────────────────────────────

func TestClose(t *testing.T) {
	bus := New()
	ctx := context.Background()
	sub := bus.SubscribeAsync(ctx, "test", HandlerFunc(func(ctx context.Context, e *Event) error {
		return nil
	}), 1)

	err := bus.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// Unsubscribe 在 Close 后再调用不应 panic（幂等）
	sub.Unsubscribe()
}

func TestCloseClosesRawChannels(t *testing.T) {
	bus := New()
	ch := bus.Subscribe(context.Background(), "test")

	bus.Close()

	// 通道应该被关闭
	_, ok := <-ch
	if ok {
		t.Error("raw channel should be closed by Close()")
	}
}

// ── Channel Full ─────────────────────────────────────────────────────────────

func TestChannelFullDrop(t *testing.T) {
	bus := New()
	ctx := context.Background()

	ch := bus.Subscribe(ctx, "test")
	// 默认通道容量 64，发布 65 个事件，最后一个应被丢弃
	for i := range 65 {
		bus.PublishTopic(ctx, "test", i)
	}

	// 消费 64 个事件
	for i := range 64 {
		select {
		case e := <-ch:
			if e.Payload != i {
				t.Errorf("event %d payload = %v, want %v", i, e.Payload, i)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out at event %d", i)
		}
	}

	// 第 65 个事件应被丢弃
	select {
	case <-ch:
		t.Error("65th event should have been dropped (channel full)")
	case <-time.After(100 * time.Millisecond):
	}
}

// ── Concurrency ──────────────────────────────────────────────────────────────

func TestConcurrentPublishSubscribe(t *testing.T) {
	bus := New()
	ctx := context.Background()

	var received atomic.Int32
	bus.SubscribeAsync(ctx, "concurrent", HandlerFunc(func(ctx context.Context, e *Event) error {
		received.Add(1)
		return nil
	}), 4)

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bus.PublishTopic(ctx, "concurrent", i)
		}(0)
	}
	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	if n := received.Load(); n != 50 {
		t.Errorf("received %d events, want 50", n)
	}
}

func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	bus := New()
	ctx := context.Background()

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub := bus.SubscribeAsync(ctx, "dynamic", HandlerFunc(func(ctx context.Context, e *Event) error {
				return nil
			}), 1)
			time.Sleep(10 * time.Millisecond)
			sub.Unsubscribe()
		}()
	}

	// 与此同时持续发布
	for range 50 {
		bus.PublishTopic(ctx, "dynamic", nil)
	}

	wg.Wait()
}

// ── Context Cancel ───────────────────────────────────────────────────────────

func TestSubscribeAsyncContextCancel(t *testing.T) {
	bus := New()
	ctx, cancel := context.WithCancel(context.Background())

	var count atomic.Int32
	bus.SubscribeAsync(ctx, "ctx.cancel", HandlerFunc(func(ctx context.Context, e *Event) error {
		count.Add(1)
		return nil
	}), 2)

	bus.PublishTopic(context.Background(), "ctx.cancel", nil)
	time.Sleep(50 * time.Millisecond)
	cancel() // 取消 context，订阅应自动清理
	time.Sleep(50 * time.Millisecond)

	// 取消后再发布，不应处理
	bus.PublishTopic(context.Background(), "ctx.cancel", nil)
	time.Sleep(50 * time.Millisecond)

	if n := count.Load(); n != 1 {
		t.Errorf("count = %d, want 1 (only pre-cancel event)", n)
	}
}

// ── PublishTopic ─────────────────────────────────────────────────────────────

func TestPublishTopic(t *testing.T) {
	bus := New()
	ctx := context.Background()
	ch := bus.Subscribe(ctx, "some.topic")

	bus.PublishTopic(ctx, "some.topic", map[string]int{"a": 1})

	e := <-ch
	if e.Source != "local" {
		t.Errorf("Source = %q, want local", e.Source)
	}
}

// ── Handler Panic ────────────────────────────────────────────────────────────

func TestHandlerPanic(t *testing.T) {
	bus := New()
	ctx := context.Background()

	var survived atomic.Bool
	bus.SubscribeAsync(ctx, "panic", HandlerFunc(func(ctx context.Context, e *Event) error {
		panic("boom")
	}), 1)

	bus.PublishTopic(ctx, "panic", nil)
	time.Sleep(100 * time.Millisecond)

	// panic 被恢复后总线继续工作
	ch := bus.Subscribe(ctx, "normal")
	bus.PublishTopic(ctx, "normal", "ok")
	select {
	case e := <-ch:
		survived.Store(true)
		_ = e
	case <-time.After(time.Second):
	}

	if !survived.Load() {
		t.Error("bus should survive handler panic")
	}
}

// ── RemoveChannel ────────────────────────────────────────────────────────────

func TestRemoveChannel(t *testing.T) {
	bus := New()
	ch := bus.Subscribe(context.Background(), "test")
	bus.RemoveChannel("test", ch)

	bus.PublishTopic(context.Background(), "test", nil)
	select {
	case <-ch:
		t.Error("channel should have been removed, should not receive events")
	case <-time.After(100 * time.Millisecond):
	}
}
