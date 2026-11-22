package amqp

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/kcersing/eventbus"
	"github.com/kcersing/eventbus/ext/amqp/amqpclt"
)

// fakeMQ 是仅实现 Publish 的假 MQ 客户端，用于验证 Publisher 生命周期。
type fakeMQ struct {
	mu      sync.Mutex
	count   int
	started chan struct{} // 可选：Publish 开始时通知
	release chan struct{} // 可选：Publish 阻塞直到关闭
}

func (f *fakeMQ) Publish(_ context.Context, _, _ string, _ amqpclt.Message) error {
	if f.started != nil {
		select {
		case f.started <- struct{}{}:
		default:
		}
	}
	if f.release != nil {
		<-f.release
	}
	f.mu.Lock()
	f.count++
	f.mu.Unlock()
	return nil
}

func (f *fakeMQ) published() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count
}

// silenceLogs 关闭本包测试期间的 Info/Warn 日志，避免并发用例刷屏。
func silenceLogs(t *testing.T) {
	t.Helper()
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(old) })
}

// Close 幂等；重复调用不 panic。
func TestPublisherCloseIsIdempotent(t *testing.T) {
	silenceLogs(t)
	bus := eventbus.New()
	defer bus.Close()

	p := newPublisher(bus, &fakeMQ{})
	if err := p.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// Close 之后调用发布方法不 panic；MQ 侧不再收到事件，本地发布仍可用。
func TestPublisherPublishAfterClose(t *testing.T) {
	silenceLogs(t)
	bus := eventbus.New()
	defer bus.Close()

	fake := &fakeMQ{}
	p := newPublisher(bus, fake)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ctx := context.Background()
	p.Local(ctx, "after.close", "local")
	if err := p.Distributed(ctx, "after.close", "distributed"); err != nil {
		t.Fatalf("Distributed after Close: %v", err)
	}
	if err := p.MQOnly(ctx, "after.close", "mq-only"); err != nil {
		t.Fatalf("MQOnly after Close: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	if got := fake.published(); got != 0 {
		t.Fatalf("MQ received %d events after Close, want 0", got)
	}
}

// Close 会先把队列中已入队的事件发完（drain），再返回。
func TestPublisherCloseDrainsQueuedEvents(t *testing.T) {
	silenceLogs(t)
	bus := eventbus.New()
	defer bus.Close()

	fake := &fakeMQ{}
	p := newPublisher(bus, fake)
	for i := 0; i < 10; i++ {
		if err := p.Distributed(context.Background(), "drain", i); err != nil {
			t.Fatalf("Distributed: %v", err)
		}
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := fake.published(); got != 10 {
		t.Fatalf("MQ received %d events, want 10", got)
	}
}

// 在途发送未完成时 Close 必须等待，而不是提前返回。
func TestPublisherCloseWaitsForInflight(t *testing.T) {
	silenceLogs(t)
	bus := eventbus.New()
	defer bus.Close()

	fake := &fakeMQ{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	p := newPublisher(bus, fake)
	if err := p.MQOnly(context.Background(), "inflight", nil); err != nil {
		t.Fatalf("MQOnly: %v", err)
	}

	select {
	case <-fake.started:
	case <-time.After(2 * time.Second):
		t.Fatal("MQ worker did not start publishing")
	}

	closed := make(chan struct{})
	go func() {
		_ = p.Close()
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("Close returned while MQ publish was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(fake.release)

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after in-flight publish finished")
	}
}

// Close 与并发发布/重复 Close 混跑：不得出现 send on closed channel 或 WaitGroup 误用。
func TestPublisherConcurrentCloseAndPublish(t *testing.T) {
	silenceLogs(t)
	bus := eventbus.New()
	defer bus.Close()

	p := newPublisher(bus, &fakeMQ{})

	const publishers = 8
	const messages = 100
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < messages; j++ {
				_ = p.Distributed(context.Background(), "race", j)
				_ = p.MQOnly(context.Background(), "race", j)
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = p.Close()
			_ = p.Close()
		}()
	}

	close(start)
	wg.Wait()

	if err := p.Close(); err != nil {
		t.Fatalf("final Close: %v", err)
	}
}

// nil *amqpclt.Publish 必须退化为纯本地发布器（接口 nil 判定不能失效）。
func TestNewPublisherWithNilMQ(t *testing.T) {
	silenceLogs(t)
	bus := eventbus.New()
	defer bus.Close()

	p := NewPublisher(bus, nil)
	if p.amqpPub != nil {
		t.Fatal("nil *amqpclt.Publish must become a nil mqPublisher interface")
	}
	if p.mqQueue != nil {
		t.Fatal("pure local publisher must not start MQ queue/worker")
	}

	ch := bus.Subscribe(context.Background(), "local.only")
	if err := p.Distributed(context.Background(), "local.only", "payload"); err != nil {
		t.Fatalf("Distributed: %v", err)
	}
	select {
	case e := <-ch:
		if e.Source != "local" {
			t.Fatalf("Source = %q, want local", e.Source)
		}
	case <-time.After(time.Second):
		t.Fatal("local subscriber did not receive event")
	}

	if err := p.MQOnly(context.Background(), "local.only", nil); err != errNoAMQP {
		t.Fatalf("MQOnly error = %v, want %v", err, errNoAMQP)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
