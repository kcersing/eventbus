package manager

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kcersing/eventbus"
	extamqp "github.com/kcersing/eventbus/ext/amqp"
)

type ctxKey struct{}

func noopHandler(context.Context, *eventbus.Event) error { return nil }

// fakeListener 实现 manager.Lifecycle，用于测试 Manager 的 Listener 分支。
type fakeListener struct {
	mu         sync.Mutex
	startCalls int
	stopCalls  int
	startErr   error
	onStart    func(context.Context)
	onStop     func()
}

func (f *fakeListener) Start(ctx context.Context) error {
	f.mu.Lock()
	f.startCalls++
	f.mu.Unlock()
	if f.onStart != nil {
		f.onStart(ctx)
	}
	return f.startErr
}

func (f *fakeListener) Stop() {
	f.mu.Lock()
	f.stopCalls++
	f.mu.Unlock()
	if f.onStop != nil {
		f.onStop()
	}
}

func (f *fakeListener) counts() (start, stop int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.startCalls, f.stopCalls
}

// waitFor 轮询等待条件成立，超时则失败。
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
		t.Fatalf("timed out waiting for %s", desc)
	}
}

// Registry 启动的消费者应真正消费总线事件。
func TestRegistryStartAllConsumesEvents(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()

	var handled atomic.Int32
	r := NewRegistry()
	r.Register("order.created", eventbus.HandlerFunc(func(context.Context, *eventbus.Event) error {
		handled.Add(1)
		return nil
	}), 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := r.StartAll(ctx, bus); err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	defer r.Shutdown()

	for i := 0; i < 3; i++ {
		bus.PublishTopic(ctx, "order.created", i)
	}
	waitFor(t, 2*time.Second, "3 events handled", func() bool { return handled.Load() == 3 })
}

// StartAll 重复调用应返回错误。
func TestRegistryStartAllRejectsSecondStart(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()

	r := NewRegistry()
	r.Register("t", eventbus.HandlerFunc(noopHandler), 1)

	ctx := context.Background()
	if err := r.StartAll(ctx, bus); err != nil {
		t.Fatalf("first StartAll: %v", err)
	}
	defer r.Shutdown()

	err := r.StartAll(ctx, bus)
	if err == nil {
		t.Fatal("expected error on second StartAll")
	}
	if !strings.Contains(err.Error(), "already started") {
		t.Fatalf("error = %v, want contains already started", err)
	}
}

// Shutdown 后消费者应停止，不再接收事件。
func TestRegistryShutdownStopsConsumers(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()

	var handled atomic.Int32
	r := NewRegistry()
	r.Register("t", eventbus.HandlerFunc(func(context.Context, *eventbus.Event) error {
		handled.Add(1)
		return nil
	}), 1)

	ctx := context.Background()
	if err := r.StartAll(ctx, bus); err != nil {
		t.Fatalf("StartAll: %v", err)
	}

	bus.PublishTopic(ctx, "t", nil)
	waitFor(t, 2*time.Second, "first event handled", func() bool { return handled.Load() == 1 })

	if err := r.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	bus.PublishTopic(ctx, "t", nil)
	time.Sleep(100 * time.Millisecond)
	if got := handled.Load(); got != 1 {
		t.Fatalf("handler called after Shutdown: got %d, want 1", got)
	}
}

// 未启动时 Shutdown 应安全返回。
func TestRegistryShutdownWithoutStart(t *testing.T) {
	if err := NewRegistry().Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// 父 ctx 的 Value / Deadline 应经 Registry 传到 Handler。
func TestRegistryPropagatesContext(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()

	got := make(chan context.Context, 1)
	r := NewRegistry()
	r.Register("ctx.topic", eventbus.HandlerFunc(func(ctx context.Context, _ *eventbus.Event) error {
		got <- ctx
		return nil
	}), 1)

	deadline := time.Now().Add(time.Minute)
	parent, cancel := context.WithDeadline(
		context.WithValue(context.Background(), ctxKey{}, "payload"), deadline)
	defer cancel()

	if err := r.StartAll(parent, bus); err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	defer r.Shutdown()

	bus.PublishTopic(context.Background(), "ctx.topic", nil)

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

// Manager 组合 Bus + Registry 时：Start 启动消费者，Shutdown 后停止消费。
func TestManagerLifecycleWithRegistry(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()

	var handled atomic.Int32
	r := NewRegistry()
	r.Register("m.topic", eventbus.HandlerFunc(func(context.Context, *eventbus.Event) error {
		handled.Add(1)
		return nil
	}), 2)

	m := New(bus, nil, nil, r)
	ctx := context.Background()

	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	bus.PublishTopic(ctx, "m.topic", nil)
	waitFor(t, 2*time.Second, "event handled", func() bool { return handled.Load() == 1 })

	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	bus.PublishTopic(ctx, "m.topic", nil)
	time.Sleep(100 * time.Millisecond)
	if got := handled.Load(); got != 1 {
		t.Fatalf("handler called after Shutdown: got %d, want 1", got)
	}
}

// Registry 启动失败时，Manager.Start 应把错误透传出来。
func TestManagerStartPropagatesRegistryError(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()

	r := NewRegistry()
	r.Register("t", eventbus.HandlerFunc(noopHandler), 1)

	ctx := context.Background()
	if err := r.StartAll(ctx, bus); err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	defer r.Shutdown()

	m := New(bus, nil, nil, r)
	if err := m.Start(ctx); err == nil {
		t.Fatal("expected error from already-started registry")
	}
}

// 全部组件为 nil 时，Start / Shutdown 应安全返回。
func TestManagerNilComponents(t *testing.T) {
	m := New(nil, nil, nil, nil)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// Shutdown 必须关闭总线：直接 Subscribe 的原始通道应被关闭；重复 Shutdown 幂等。
func TestManagerShutdownClosesBus(t *testing.T) {
	bus := eventbus.New()
	ch := bus.Subscribe(context.Background(), "raw.topic")

	m := New(bus, nil, nil, nil)
	ctx := context.Background()
	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unexpected event from closed bus channel")
		}
	case <-time.After(time.Second):
		t.Fatal("bus channel not closed after Shutdown")
	}

	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

// Shutdown 应关闭 Publisher（此处为无 MQ 的本地 Publisher），且不 panic。
func TestManagerShutdownClosesPublisher(t *testing.T) {
	bus := eventbus.New()
	pub := extamqp.NewPublisher(bus, nil)

	m := New(bus, nil, pub, nil)
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// Manager 应把 ctx 传给 Listener.Start，并在 Shutdown 时调用 Stop。
func TestManagerListenerLifecycle(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()

	var startedCtx context.Context
	l := &fakeListener{onStart: func(ctx context.Context) { startedCtx = ctx }}

	m := New(bus, l, nil, nil)
	ctx := context.WithValue(context.Background(), ctxKey{}, "listener")

	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if startedCtx == nil || startedCtx.Value(ctxKey{}) != "listener" {
		t.Fatalf("listener ctx value = %v, want listener", startedCtx)
	}

	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	start, stop := l.counts()
	if start != 1 || stop != 1 {
		t.Fatalf("listener start/stop = %d/%d, want 1/1", start, stop)
	}
}

// Listener.Start 失败时 Manager.Start 应返回错误，且不启动 Registry。
func TestManagerListenerStartErrorSkipsRegistry(t *testing.T) {
	bus := eventbus.New()
	defer bus.Close()

	var handled atomic.Int32
	r := NewRegistry()
	r.Register("t", eventbus.HandlerFunc(func(context.Context, *eventbus.Event) error {
		handled.Add(1)
		return nil
	}), 1)

	l := &fakeListener{startErr: errors.New("listener boom")}
	m := New(bus, l, nil, r)

	err := m.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "listener boom") {
		t.Fatalf("Start error = %v, want contains listener boom", err)
	}

	bus.PublishTopic(context.Background(), "t", nil)
	time.Sleep(100 * time.Millisecond)
	if got := handled.Load(); got != 0 {
		t.Fatalf("registry consumer ran after listener start error: got %d, want 0", got)
	}

	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if _, stop := l.counts(); stop != 1 {
		t.Fatalf("listener stop calls = %d, want 1", stop)
	}
}

// Shutdown 顺序要求 Listener 先于 Bus 停止。
func TestManagerShutdownStopsListenerBeforeBus(t *testing.T) {
	bus := eventbus.New()
	ch := bus.Subscribe(context.Background(), "raw.topic")

	var busClosedDuringStop atomic.Bool
	l := &fakeListener{onStop: func() {
		select {
		case <-ch:
			busClosedDuringStop.Store(true)
		default:
			busClosedDuringStop.Store(false)
		}
	}}

	m := New(bus, l, nil, nil)
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if busClosedDuringStop.Load() {
		t.Fatal("bus was closed before listener.Stop")
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unexpected event from closed bus channel")
		}
	case <-time.After(time.Second):
		t.Fatal("bus channel not closed after Shutdown")
	}
}
