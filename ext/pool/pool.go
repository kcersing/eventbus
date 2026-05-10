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

	"eventbus"
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

func newPool(name string, handler eventbus.Handler, workerNum int, opts Options) *ConsumerPool {
	ctx, cancel := context.WithCancel(context.Background())
	cp := &ConsumerPool{
		name:    name,
		handler: handler,
		queue:   make(chan *eventbus.Event, opts.QueueSize),
		opts:    opts,
		ctx:     ctx,
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

	cp := newPool(topic, handler, workerNum, o)
	ch := bus.Subscribe(ctx, topic)
	ctx, cancel := context.WithCancel(ctx)

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
		for {
			select {
			case <-ctx.Done():
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
}

func (s *poolSub) Unsubscribe() {
	s.once.Do(func() {
		s.cancel()    // 停止转发 goroutine
		s.wg.Wait()   // 等待转发 goroutine 退出
		s.pool.stop() // 等待所有 worker 退出
		s.bus.RemoveChannel(s.topic, s.ch)
		closeEventChan(s.ch)
		logInfo("pool: unsubscribed: topic=%s", s.topic)
	})
}

func closeEventChan(ch eventbus.EventChan) {
	defer func() { recover() }()
	close(ch)
}
