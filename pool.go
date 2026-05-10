package eventbus

import (
	"context"
	"sync"
	"time"
)

// FailHandler 定义了当事件无法入队时的处理函数
type FailHandler func(event *Event, err error)

// DeadLetterFunc 重试耗尽后的死信处理（如写入死信 topic、落盘等）
type DeadLetterFunc func(event *Event)

// PoolOptions 定义了消费者池的配置选项
type PoolOptions struct {
	QueueSize      int
	HandlerTimeout time.Duration
	FailHandler    FailHandler
	MaxRetries     int            // 最大重试次数（0=不重试）
	RetryBackoff   time.Duration  // 重试基础间隔（默认 1s，指数退避）
	DeadLetterFunc DeadLetterFunc // 重试耗尽回调
}

// WithQueueSize 设置消费者池的队列大小
func WithQueueSize(size int) func(*PoolOptions) {
	return func(o *PoolOptions) { o.QueueSize = size }
}

// WithHandlerTimeout 设置处理器的超时时间
func WithHandlerTimeout(timeout time.Duration) func(*PoolOptions) {
	return func(o *PoolOptions) { o.HandlerTimeout = timeout }
}

// WithFailHandler 设置自定义的失败处理器
func WithFailHandler(handler FailHandler) func(*PoolOptions) {
	return func(o *PoolOptions) { o.FailHandler = handler }
}

// WithMaxRetries 设置最大重试次数
func WithMaxRetries(n int) func(*PoolOptions) {
	return func(o *PoolOptions) { o.MaxRetries = n }
}

// WithRetryBackoff 设置重试基础间隔
func WithRetryBackoff(d time.Duration) func(*PoolOptions) {
	return func(o *PoolOptions) { o.RetryBackoff = d }
}

// WithDeadLetterFunc 设置死信处理器
func WithDeadLetterFunc(fn DeadLetterFunc) func(*PoolOptions) {
	return func(o *PoolOptions) { o.DeadLetterFunc = fn }
}

// ConsumerPool 消费者池，多 worker 并发消费事件。
// 支持：超时控制、指数退避重试（MaxRetries / RetryBackoff）、死信回调（DeadLetterFunc）、Metrics 埋点。
type ConsumerPool struct {
	name      string
	handler   Handler
	workerNum int32
	queue     chan *Event
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	options   PoolOptions
	metrics   Metrics
}

// NewConsumerPool 创建消费者池
func NewConsumerPool(name string, handler Handler, workerNum int32, opts ...func(*PoolOptions)) *ConsumerPool {
	options := PoolOptions{
		QueueSize:      DefaultConfig.QueueSize,
		HandlerTimeout: DefaultConfig.HandlerTimeout,
		FailHandler: func(event *Event, err error) {
			logWarn("consumer pool queue full or closed, event dropped: pool=%s topic=%s", name, event.Topic)
		},
		MaxRetries:   0,
		RetryBackoff: time.Second,
	}
	for _, opt := range opts {
		opt(&options)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &ConsumerPool{
		name:      name,
		handler:   handler,
		workerNum: workerNum,
		queue:     make(chan *Event, options.QueueSize),
		ctx:       ctx,
		cancel:    cancel,
		options:   options,
		metrics:   noopMetrics{},
	}
}

// SetMetrics 注入自定义 Metrics 实现
func (cp *ConsumerPool) SetMetrics(m Metrics) {
	if m != nil {
		cp.metrics = m
	}
}

// Start 启动消费者池
func (cp *ConsumerPool) Start() {
	for i := int32(0); i < cp.workerNum; i++ {
		cp.wg.Add(1)
		go cp.worker()
	}
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
			cp.processEvent(event)
		}
	}
}

// processEvent 处理单个事件，含 panic 恢复、超时控制和指数退避重试。
//
// Fix #4（原始 bug）：原代码在 defer/recover 里将 panicked 置为 true，
// 但 for 循环中 `if panicked { deadLetter(); return }` 的检查在 panic 发生后
// 根本不会被执行——panic 已经让栈展开到 defer，recover 捕获后函数直接返回，
// deadLetter 永远不会被调用。
//
// 修复方案：将死信处理直接放到 recover 块内部，确保 panic 后必然触发死信。
func (cp *ConsumerPool) processEvent(event *Event) {
	defer func() {
		if r := recover(); r != nil {
			// Fix #4: panic 在此处被捕获，直接调用 deadLetter，不再依赖循环外的标志位。
			cp.metrics.IncError(event.Topic)
			logError("consumer pool panic recovered: pool=%s panic=%v", cp.name, r)
			cp.deadLetter(event)
		}
	}()

	for attempt := 0; attempt <= cp.options.MaxRetries; attempt++ {
		handlerCtx := cp.ctx
		var cancel context.CancelFunc
		if cp.options.HandlerTimeout > 0 {
			handlerCtx, cancel = context.WithTimeout(cp.ctx, cp.options.HandlerTimeout)
		}

		start := time.Now()
		err := cp.handler.Handle(handlerCtx, event)
		if cancel != nil {
			cancel()
		}
		cp.metrics.ObserveHandleDuration(event.Topic, float64(time.Since(start).Microseconds()))

		if err == nil {
			return // 成功
		}

		cp.metrics.IncError(event.Topic)

		if attempt == cp.options.MaxRetries {
			logError("consumer pool retries exhausted: pool=%s retries=%d error=%v", cp.name, cp.options.MaxRetries, err)
			cp.deadLetter(event)
			return
		}

		// 指数退避重试
		backoff := cp.options.RetryBackoff
		shift := attempt
		if shift > 10 {
			shift = 10
		}
		sleep := backoff * time.Duration(1<<shift)
		logWarn("consumer pool retry: pool=%s attempt=%d/%d topic=%s error=%v backoff=%v",
			cp.name, attempt+1, cp.options.MaxRetries, event.Topic, err, sleep)
		select {
		case <-cp.ctx.Done():
			cp.deadLetter(event)
			return
		case <-time.After(sleep):
		}
	}
}

// deadLetter 调用 DeadLetterFunc 处理重试耗尽的事件（如写入死信 topic、落盘、告警）。
func (cp *ConsumerPool) deadLetter(event *Event) {
	if cp.options.DeadLetterFunc != nil {
		cp.options.DeadLetterFunc(event)
	}
}

// Consume 将事件投递到消费者池队列。队列满时触发 FailHandler。
func (cp *ConsumerPool) Consume(event *Event) {
	select {
	case <-cp.ctx.Done():
		cp.metrics.IncDropped(event.Topic)
		cp.options.FailHandler(event, context.Canceled)
		return
	case cp.queue <- event:
		cp.metrics.SetQueueDepth(event.Topic, len(cp.queue))
	default:
		cp.metrics.IncDropped(event.Topic)
		cp.options.FailHandler(event, ErrQueueFull)
	}
}

// Stop 停止消费者池，等待所有 worker 处理完当前事件后退出。
func (cp *ConsumerPool) Stop() {
	cp.cancel()
	cp.wg.Wait()
}
