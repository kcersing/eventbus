package amqp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kcersing/eventbus"
	"github.com/kcersing/eventbus/ext/amqp/amqpclt"
)

// ConsumerOptions 是 1:1 可靠消费者的配置。
type ConsumerOptions struct {
	// ProcessTimeout 单次 Handler 处理超时，0 表示不额外限制。
	ProcessTimeout time.Duration
	// MaxRetries Handler 失败后的最大重试次数，默认 3。
	MaxRetries int
	// RetryBackoff 重试基础间隔，指数退避，默认 1s。
	RetryBackoff time.Duration
	// RetryClassifier 决定失败后的处理策略；nil 时使用 eventbus.DefaultRetryClassifier。
	RetryClassifier eventbus.RetryClassifier
	// AfterCommit 在 Handler 成功后、Ack 之前执行；失败只记录，不触发重投。
	AfterCommit []func(ctx context.Context, event *eventbus.Event) error
}

// ConsumerOption 配置 Consumer。
type ConsumerOption func(*ConsumerOptions)

func WithProcessTimeout(d time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) { o.ProcessTimeout = d }
}

func WithMaxRetries(n int) ConsumerOption {
	return func(o *ConsumerOptions) { o.MaxRetries = n }
}

func WithRetryBackoff(d time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) { o.RetryBackoff = d }
}

func WithRetryClassifier(fn eventbus.RetryClassifier) ConsumerOption {
	return func(o *ConsumerOptions) { o.RetryClassifier = fn }
}

// WithAfterCommit 注册 Handler 成功后的副作用。
//
// 这些副作用在 Ack 之前执行，但失败只会记录日志，不会 Nack 重投：因为主业务
// 已经成功，重投会导致重复执行。需要可靠副作用时，应使用本地补偿/Outbox，
// 而不是依赖 MQ 重试。
func WithAfterCommit(fn func(ctx context.Context, event *eventbus.Event) error) ConsumerOption {
	return func(o *ConsumerOptions) { o.AfterCommit = append(o.AfterCommit, fn) }
}

// Consumer 是 1:1 可靠消费者：直接消费 AMQP delivery，Handler 成功后才 Ack。
//
// 与 Listener 不同，Consumer 不向内存总线 fanout；它适合“一个业务处理器负责
// 这条消息”的场景，支持处理超时、失败分类、重试和提交后副作用。
type Consumer struct {
	subscriber manualSubscriber
	handler    eventbus.Handler
	opts       ConsumerOptions

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewConsumer 创建 1:1 可靠消费者。
func NewConsumer(subscriber *amqpclt.Subscribe, handler eventbus.Handler, opts ...ConsumerOption) *Consumer {
	return newConsumer(subscriber, handler, opts...)
}

// newConsumer 是 NewConsumer 的内部实现，接收接口以便测试注入 fake。
func newConsumer(subscriber manualSubscriber, handler eventbus.Handler, opts ...ConsumerOption) *Consumer {
	o := ConsumerOptions{
		MaxRetries:   3,
		RetryBackoff: time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return &Consumer{
		subscriber: subscriber,
		handler:    handler,
		opts:       o,
		done:       make(chan struct{}),
	}
}

// Start 启动消费循环。
func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return fmt.Errorf("amqp consumer already started")
	}
	c.started = true
	ctx, c.cancel = context.WithCancel(ctx)
	c.mu.Unlock()

	go func() {
		defer close(c.done)

		deliveries, cleanup, err := c.subscriber.SubscribeManual(ctx)
		if err != nil {
			logError("amqp consumer: subscribe failed: %v", err)
			return
		}
		defer cleanup()
		logInfo("amqp consumer: started")

		for {
			select {
			case <-ctx.Done():
				logInfo("amqp consumer: stopped")
				return
			case delivery, ok := <-deliveries:
				if !ok {
					logWarn("amqp consumer: delivery channel closed")
					return
				}
				c.handle(ctx, delivery)
			}
		}
	}()
	return nil
}

// Stop 停止消费并等待在途 Handler 退出。
func (c *Consumer) Stop() {
	c.mu.Lock()
	started := c.started
	cancel := c.cancel
	c.mu.Unlock()

	if !started || cancel == nil {
		return
	}
	cancel()
	<-c.done
}

// handle 处理单条 delivery；成功后才 Ack。
func (c *Consumer) handle(ctx context.Context, delivery *amqpclt.Delivery) {
	if delivery == nil || delivery.Message == nil {
		if delivery != nil {
			if err := delivery.Nack(false); err != nil {
				logError("amqp consumer: nack empty delivery: %v", err)
			}
		}
		return
	}

	event := eventFromMessage(delivery.Message)

	for attempt := 0; ; attempt++ {
		processCtx := ctx
		var cancel context.CancelFunc
		if c.opts.ProcessTimeout > 0 {
			processCtx, cancel = context.WithTimeout(ctx, c.opts.ProcessTimeout)
		}

		err := callHandler(processCtx, c.handler, event)
		if cancel != nil {
			cancel()
		}

		if err == nil {
			c.runAfterCommit(ctx, event)
			if ackErr := delivery.Ack(); ackErr != nil {
				logError("amqp consumer: ack failed: topic=%s event=%s error=%v", event.Topic, event.Id, ackErr)
			} else {
				logInfo("amqp consumer: processed: topic=%s event=%s attempt=%d", event.Topic, event.Id, attempt+1)
			}
			return
		}

		// 消费上下文已取消：重新入队，交给下一个实例。
		if ctx.Err() != nil {
			logWarn("amqp consumer: context canceled, requeue: topic=%s event=%s", event.Topic, event.Id)
			if nackErr := delivery.Nack(true); nackErr != nil {
				logError("amqp consumer: nack on cancel failed: %v", nackErr)
			}
			return
		}

		classifier := c.opts.RetryClassifier
		if classifier == nil {
			classifier = eventbus.DefaultRetryClassifier
		}
		switch classifier(ctx, event, err) {
		case eventbus.Drop:
			logWarn("amqp consumer: classifier=drop: topic=%s event=%s error=%v", event.Topic, event.Id, err)
			if ackErr := delivery.Ack(); ackErr != nil {
				logError("amqp consumer: ack on drop failed: %v", ackErr)
			}
			return
		case eventbus.DeadLetter:
			logWarn("amqp consumer: classifier=dead_letter: topic=%s event=%s error=%v", event.Topic, event.Id, err)
			if nackErr := delivery.Nack(false); nackErr != nil {
				logError("amqp consumer: nack on dead_letter failed: %v", nackErr)
			}
			return
		}

		if attempt >= c.opts.MaxRetries {
			logError("amqp consumer: retries exhausted: topic=%s event=%s retries=%d error=%v",
				event.Topic, event.Id, c.opts.MaxRetries, err)
			if nackErr := delivery.Nack(false); nackErr != nil {
				logError("amqp consumer: nack on exhausted failed: %v", nackErr)
			}
			return
		}

		sleep := c.opts.RetryBackoff * (1 << min(attempt, 10))
		logWarn("amqp consumer: retry: topic=%s event=%s attempt=%d/%d error=%v backoff=%v",
			event.Topic, event.Id, attempt+1, c.opts.MaxRetries, err, sleep)
		select {
		case <-ctx.Done():
			if nackErr := delivery.Nack(true); nackErr != nil {
				logError("amqp consumer: nack on cancel failed: %v", nackErr)
			}
			return
		case <-time.After(sleep):
		}
	}
}

func (c *Consumer) runAfterCommit(ctx context.Context, event *eventbus.Event) {
	for _, hook := range c.opts.AfterCommit {
		if hook == nil {
			continue
		}
		if err := hook(ctx, event); err != nil {
			// 主业务已成功，副作用失败只记录/补偿，不 Nack。
			logError("amqp consumer: after commit failed: topic=%s event=%s error=%v", event.Topic, event.Id, err)
		}
	}
}

// eventFromMessage 将 amqpclt.Message 转为 eventbus.Event。
func eventFromMessage(msg *amqpclt.Message) *eventbus.Event {
	event := &eventbus.Event{
		Id:        msg.CorrelationId,
		Topic:     msg.Event,
		Payload:   msg.Payload,
		Timestamp: msg.Timestamp,
		Source:    "amqp",
	}
	if event.Id == "" {
		event.Id = eventbus.NewEvent(event.Topic, event.Payload).Id
		logWarn("amqp consumer: message without correlation id, generated event id=%s", event.Id)
	}
	return event
}

// callHandler 执行 Handler 并恢复 panic，将 panic 转成普通错误。
func callHandler(ctx context.Context, handler eventbus.Handler, event *eventbus.Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("amqp consumer: handler panic: %v", r)
		}
	}()
	return handler.Handle(ctx, event)
}
