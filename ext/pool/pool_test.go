package pool

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcersing/eventbus"
)

type ctxKey struct{}

func noopHandler(context.Context, *eventbus.Event) error { return nil }

// waitFor 轮询等待条件成立；超时后输出当前 goroutine 数，便于定位泄漏。
func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s (goroutines=%d)", desc, runtime.NumGoroutine())
	}
}

// 父 ctx 取消后：worker 与转发 goroutine 全部退出，订阅不再接收事件。
func TestParentContextCancelStopsWorkers(t *testing.T) {
	base := runtime.NumGoroutine()
	bus := eventbus.New()
	defer bus.Close()

	var handled atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	sub := SubscribeWithPool(ctx, bus, "pool.cancel", eventbus.HandlerFunc(
		func(context.Context, *eventbus.Event) error {
			handled.Add(1)
			return nil
		}), 4)

	// 1 个转发 goroutine + 4 个 worker
	waitFor(t, 2*time.Second, "pool goroutines started",
		func() bool { return runtime.NumGoroutine() >= base+5 })

	bus.PublishTopic(context.Background(), "pool.cancel", nil)
	waitFor(t, 2*time.Second, "first event handled", func() bool { return handled.Load() == 1 })

	cancel()
	waitFor(t, 2*time.Second, "pool goroutines stopped",
		func() bool { return runtime.NumGoroutine() <= base })

	bus.PublishTopic(context.Background(), "pool.cancel", nil)
	time.Sleep(50 * time.Millisecond)
	if got := handled.Load(); got != 1 {
		t.Fatalf("handler called after ctx cancel: got %d, want 1", got)
	}

	sub.Unsubscribe() // 取消后仍应可安全调用
}

// 总线 Close() 关闭内部通道后：池 worker 必须被回收，且 Unsubscribe 不 panic。
func TestBusCloseStopsWorkers(t *testing.T) {
	base := runtime.NumGoroutine()
	bus := eventbus.New()
	sub := SubscribeWithPool(context.Background(), bus, "pool.close", eventbus.HandlerFunc(noopHandler), 3)

	waitFor(t, 2*time.Second, "pool goroutines started",
		func() bool { return runtime.NumGoroutine() >= base+4 })

	bus.Close()
	waitFor(t, 2*time.Second, "pool goroutines stopped after bus.Close",
		func() bool { return runtime.NumGoroutine() <= base })

	sub.Unsubscribe() // 总线已关闭，仍应幂等且不 panic
}

// 父 ctx 的 Value / Deadline 必须传递到 Handler。
func TestHandlerReceivesParentContext(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()

	deadline := time.Now().Add(time.Minute)
	parent, cancel := context.WithDeadline(
		context.WithValue(context.Background(), ctxKey{}, "payload"), deadline)
	defer cancel()

	got := make(chan context.Context, 1)
	sub := SubscribeWithPool(parent, bus, "pool.ctx", eventbus.HandlerFunc(
		func(ctx context.Context, _ *eventbus.Event) error {
			got <- ctx
			return nil
		}), 1)
	defer sub.Unsubscribe()

	bus.PublishTopic(context.Background(), "pool.ctx", nil)

	select {
	case ctx := <-got:
		if v := ctx.Value(ctxKey{}); v != "payload" {
			t.Fatalf("handler ctx value = %v, want payload", v)
		}
		dl, ok := ctx.Deadline()
		if !ok || !dl.Equal(deadline) {
			t.Fatalf("handler ctx deadline = %v (ok=%v), want %v", dl, ok, deadline)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called")
	}
}

// Unsubscribe 幂等；取消后订阅不再接收事件，资源全部释放。
func TestUnsubscribeIdempotent(t *testing.T) {
	base := runtime.NumGoroutine()
	bus := eventbus.New()
	defer bus.Close()

	var handled atomic.Int32
	sub := SubscribeWithPool(context.Background(), bus, "pool.unsub", eventbus.HandlerFunc(
		func(context.Context, *eventbus.Event) error {
			handled.Add(1)
			return nil
		}), 2)

	waitFor(t, 2*time.Second, "pool goroutines started",
		func() bool { return runtime.NumGoroutine() >= base+3 })

	sub.Unsubscribe()
	sub.Unsubscribe() // 第二次必须幂等

	waitFor(t, 2*time.Second, "pool goroutines stopped",
		func() bool { return runtime.NumGoroutine() <= base })

	bus.PublishTopic(context.Background(), "pool.unsub", nil)
	time.Sleep(50 * time.Millisecond)
	if got := handled.Load(); got != 0 {
		t.Fatalf("handler called after Unsubscribe: got %d, want 0", got)
	}
}
