// Package pool 提供高级消费者池，作为 eventbus 核心包的可选扩展。
//
// 功能：并发 worker、超时控制、指数退避重试、死信回调。
// 零侵入：核心包不依赖此包，可按需引入。
//
// 用法：
//
//	sub := pool.SubscribeWithPool(ctx, bus, "order.created", handler, 4,
//	    pool.WithMaxRetries(3),
//	    pool.WithDeadLetterFunc(func(e *eventbus.Event) { /* 落盘 */ }),
//	)
//	defer sub.Unsubscribe()
package pool

import (
	"context"
	"sync"
	"time"

	"github.com/kcersing/eventbus"
)

// Options 消费者池配置。
type Options struct {
	// QueueSize 内部队列容量，默认 1000。
	QueueSize int
	// HandlerTimeout 单次处理超时，0 表示无超时。
	HandlerTimeout time.Duration
	// MaxRetries 最大重试次数，0 表示不重试。
	MaxRetries int
	// RetryBackoff 重试基础间隔，指数退避，默认 1s。
	RetryBackoff time.Duration
	// RetryClassifier 决定失败后的处理策略；nil 时使用 eventbus.DefaultRetryClassifier。
	RetryClassifier eventbus.RetryClassifier
	// FailHandler 入队失败（队列满/已关闭）时的回调。
	FailHandler func(event *eventbus.Event, err error)
	// DeadLetterFunc 重试耗尽时的死信回调（如落盘、告警）。
	DeadLetterFunc func(event *eventbus.Event)
}

// Option 函数式配置项。
type Option func(*Options)

func WithQueueSize(n int) Option                { return func(o *Options) { o.QueueSize = n } }
func WithHandlerTimeout(d time.Duration) Option { return func(o *Options) { o.HandlerTimeout = d } }
func WithMaxRetries(n int) Option               { return func(o *Options) { o.MaxRetries = n } }
func WithRetryBackoff(d time.Duration) Option   { return func(o *Options) { o.RetryBackoff = d } }
func WithRetryClassifier(fn eventbus.RetryClassifier) Option {
	return func(o *Options) { o.RetryClassifier = fn }
}
func WithFailHandler(fn func(*eventbus.Event, error)) Option {
	return func(o *Options) { o.FailHandler = fn }
}
func WithDeadLetterFunc(fn func(*eventbus.Event)) Option {
	return func(o *Options) { o.DeadLetterFunc = fn }
}

// ConsumerPool 多 worker 并发消费者池。
type ConsumerPool struct {
	name    string
	handler eventbus.Handler
	queue   chan *eventbus.Event
	opts    Options
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// newPool 创建消费者池。ctx 是池的生命周期根：其取消会立即停止所有 worker，
// 其 Value/Deadline 也会传递给 Handler 的 ctx。
func newPool(ctx context.Context, name string, handler eventbus.Handler, workerNum int, opts Options) *ConsumerPool {
	poolCtx, cancel := context.WithCancel(ctx)
	cp := &ConsumerPool{
		name:    name,
		handler: handler,
		queue:   make(chan *eventbus.Event, opts.QueueSize),
		opts:    opts,
		ctx:     poolCtx,
		cancel:  cancel,
	}
	for range workerNum {
		cp.wg.Add(1)
		go cp.worker()
	}
	return cp
}

func (cp *ConsumerPool) worker() {
	defer cp.wg.Done()
	for {
		select {
		case <-cp.ctx.Done():
			return
		case event, ok := <-cp.queue:
			if !ok {
				return
			}
			cp.process(event)
		}
	}
}

// process 处理单个事件，含 panic 恢复、超时控制和指数退避重试。
func (cp *ConsumerPool) process(event *eventbus.Event) {
	defer func() {
		if r := recover(); r != nil {
			logError("pool: panic recovered: pool=%s topic=%s panic=%v", cp.name, event.Topic, r)
			cp.deadLetter(event)
		}
	}()

	for attempt := range cp.opts.MaxRetries + 1 {
		ctx := cp.ctx
		var cancelFn context.CancelFunc
		if cp.opts.HandlerTimeout > 0 {
			ctx, cancelFn = context.WithTimeout(cp.ctx, cp.opts.HandlerTimeout)
		}

		err := cp.handler.Handle(ctx, event)
		if cancelFn != nil {
			cancelFn()
		}

		if err == nil {
			return
		}

		classifier := cp.opts.RetryClassifier
		if classifier == nil {
			classifier = eventbus.DefaultRetryClassifier
		}
		switch classifier(cp.ctx, event, err) {
		case eventbus.DeadLetter:
			logWarn("pool: classifier=dead_letter: pool=%s topic=%s error=%v", cp.name, event.Topic, err)
			cp.deadLetter(event)
			return
		case eventbus.Drop:
			logWarn("pool: classifier=drop: pool=%s topic=%s error=%v", cp.name, event.Topic, err)
			return
		}

		if attempt == cp.opts.MaxRetries {
			logError("pool: retries exhausted: pool=%s topic=%s retries=%d error=%v",
				cp.name, event.Topic, cp.opts.MaxRetries, err)
			cp.deadLetter(event)
			return
		}

		sleep := cp.opts.RetryBackoff * (1 << min(attempt, 10))
		logWarn("pool: retry: pool=%s topic=%s attempt=%d/%d error=%v backoff=%v",
			cp.name, event.Topic, attempt+1, cp.opts.MaxRetries, err, sleep)
		select {
		case <-cp.ctx.Done():
			cp.deadLetter(event)
			return
		case <-time.After(sleep):
		}
	}
}

func (cp *ConsumerPool) consume(event *eventbus.Event) {
	select {
	case <-cp.ctx.Done():
		cp.opts.FailHandler(event, context.Canceled)
	case cp.queue <- event:
	default:
		cp.opts.FailHandler(event, errQueueFull)
	}
}

func (cp *ConsumerPool) stop() {
	cp.cancel()
	cp.wg.Wait()
}

func (cp *ConsumerPool) deadLetter(event *eventbus.Event) {
	if cp.opts.DeadLetterFunc != nil {
		cp.opts.DeadLetterFunc(event)
	}
}

// SubscribeWithPool 使用消费者池订阅事件，返回可取消的 Subscription。
//
// workerNum 控制并发 worker 数量；通过 Option 可配置重试、超时、死信等行为。
//
// 生命周期：父 ctx 取消、Unsubscribe、总线 Close 三者任一发生都会停止 worker、
// 移除订阅并关闭内部通道；Unsubscribe 可重复调用。父 ctx 的 Value/Deadline 会
// 传递给 Handler。注意：停止时队列中尚未处理的事件会被丢弃；Handler 内不要同步
// 调用 Unsubscribe（会等待自身退出而阻塞）。
func SubscribeWithPool(
	ctx context.Context,
	bus *eventbus.EventBus,
	topic string,
	handler eventbus.Handler,
	workerNum int,
	opts ...Option,
) eventbus.Subscription {
	o := Options{
		QueueSize:    1000,
		RetryBackoff: time.Second,
		FailHandler: func(event *eventbus.Event, err error) {
			logWarn("pool: event dropped: topic=%s error=%v", event.Topic, err)
		},
	}
	for _, opt := range opts {
		opt(&o)
	}

	cp := newPool(ctx, topic, handler, workerNum, o)
	ch := bus.Subscribe(ctx, topic)
	fwdCtx, cancel := context.WithCancel(ctx)

	sub := &poolSub{
		bus:    bus,
		topic:  topic,
		ch:     ch,
		pool:   cp,
		cancel: cancel,
	}

	sub.wg.Add(1)
	go func() {
		defer sub.wg.Done()
		// 统一收敛：父 ctx 取消、总线关闭（ch 被关闭）、主动 Unsubscribe 都会走到这里，
		// 避免任一退出路径留下 worker 泄漏。
		defer sub.teardown()
		for {
			select {
			case <-fwdCtx.Done():
				return
			case event, ok := <-ch:
				if !ok {
					return
				}
				cp.consume(event)
			}
		}
	}()

	return sub
}

// poolSub 是 SubscribeWithPool 返回的 Subscription 实现。
type poolSub struct {
	bus    *eventbus.EventBus
	topic  string
	ch     eventbus.EventChan
	pool   *ConsumerPool
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
	closed sync.Once
}

// Unsubscribe 取消订阅并释放资源，可重复调用。
func (s *poolSub) Unsubscribe() {
	s.once.Do(func() {
		s.cancel()   // 停止转发 goroutine
		s.wg.Wait()  // 等待转发 goroutine 退出（其 defer 会执行 teardown）
		s.teardown() // 兜底：确保资源在任何退出路径下都被释放
	})
}

// teardown 释放订阅资源。转发 goroutine 与 Unsubscribe 都可能触发它，
// 通过 sync.Once 保证只执行一次；不等待转发 goroutine，避免自等待死锁。
func (s *poolSub) teardown() {
	s.closed.Do(func() {
		s.bus.RemoveChannel(s.topic, s.ch) // 先停止新事件投递
		s.pool.stop()                      // 等待所有 worker 退出
		closeEventChan(s.ch)
		logInfo("pool: unsubscribed: topic=%s", s.topic)
	})
}

func closeEventChan(ch eventbus.EventChan) {
	defer func() { recover() }()
	close(ch)
}
