package eventbus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kcersing/amqpclt"
)

// PublishScope 定义事件的发布范围。
type PublishScope int

const (
	// ScopeLocal 仅发布到本地内存总线（默认）。
	ScopeLocal PublishScope = 1
	// ScopeDistributed 同时发布到 MQ 和本地内存总线，注意本地订阅者会收到事件。
	ScopeDistributed PublishScope = 2
	// ScopeMQOnly 仅发布到 MQ，本地订阅者不会收到。
	ScopeMQOnly PublishScope = 3
)

// PublishOptions 定义了发布的选项
type PublishOptions struct {
	Scope PublishScope
}

// WithScope 返回一个设置发布范围的 Option 函数。
func WithScope(scope PublishScope) func(*PublishOptions) {
	return func(o *PublishOptions) {
		o.Scope = scope
	}
}

// EventPublisher 统一事件发布入口，根据 Scope 将事件路由到内存总线和/或 MQ。
// 内部使用 mqQueue + 后台 worker 批量推送到 MQ，队列满时通过 semaphore 降级为异步直接发送。
type EventPublisher struct {
	// Fix #5: wg 同时追踪 mqWorker 和所有降级 goroutine，Close 等待全部退出后再返回，
	// 避免降级 goroutine 在 amqpPub 已关闭后继续使用而引发 panic。
	wg          sync.WaitGroup
	memoryBus   *EventBus        // 内存事件总线
	amqpPub     *amqpclt.Publish // AMQP 发布者（可选）
	mqQueue     chan *Event       // 本地队列，由后台 worker 负责推送到 MQ
	fallbackSem chan struct{}     // 降级 goroutine 信号量，限制并发数
}

// NewEventPublisher 创建事件发布管理器
func NewEventPublisher(memoryBus *EventBus, amqpPub *amqpclt.Publish) *EventPublisher {
	pub := &EventPublisher{
		memoryBus: memoryBus,
		amqpPub:   amqpPub,
	}
	if amqpPub != nil {
		pub.mqQueue = make(chan *Event, DefaultConfig.QueueSize)
		pub.fallbackSem = make(chan struct{}, 100) // 降级 goroutine 上限
		pub.wg.Add(1)
		go func() {
			defer pub.wg.Done()
			pub.startMQWorker()
		}()
	}
	return pub
}

// SetMetrics 注入自定义 Metrics 实现
func (pub *EventPublisher) SetMetrics(m Metrics) {
	if m != nil {
		pub.memoryBus.SetMetrics(m)
	}
}

// Publish 是统一的事件发布方法
func (pub *EventPublisher) Publish(ctx context.Context, topic string, payload any, opts ...func(*PublishOptions)) error {
	options := &PublishOptions{
		Scope: ScopeLocal,
	}
	for _, opt := range opts {
		opt(options)
	}

	event := NewEvent(topic, payload)

	var err error
	switch options.Scope {
	case ScopeLocal:
		event.Source = "local"
		pub.memoryBus.Publish(ctx, event)
		pub.memoryBus.metrics.IncPublished(topic)
		logInfo("event published to memory bus: topic=%s eventId=%s", topic, event.Id)

	case ScopeDistributed:
		if pub.amqpPub == nil {
			logWarn("AMQP not configured, distributed scope downgraded to local: topic=%s", topic)
			return pub.Publish(ctx, topic, payload, WithScope(ScopeLocal))
		}
		event.Source = "distributed"
		pub.enqueueMQ(ctx, event)
		pub.memoryBus.Publish(ctx, event)
		pub.memoryBus.metrics.IncPublished(topic)
		logInfo("event enqueued to MQ and published to memory bus: topic=%s eventId=%s", topic, event.Id)

	case ScopeMQOnly:
		if pub.amqpPub == nil {
			return fmt.Errorf("AMQP publisher not configured, cannot publish MQ-only event: topic=%s", topic)
		}
		event.Source = "amqp"
		pub.enqueueMQ(ctx, event)
		pub.memoryBus.metrics.IncPublished(topic)

	default:
		err = fmt.Errorf("unknown publish scope: %v", options.Scope)
	}

	return err
}

// enqueueMQ 将事件放入 MQ 队列；队列满时降级为受信号量限制的直接异步发送。
//
// Fix #5: 降级 goroutine 通过 pub.wg.Add(1) 纳入 WaitGroup 统一追踪，
// 确保 Close() 调用 pub.wg.Wait() 时能等待所有降级 goroutine 完成，
// 防止 amqpPub 关闭后降级 goroutine 继续写入而引发 panic 或数据竞争。
func (pub *EventPublisher) enqueueMQ(ctx context.Context, event *Event) {
	select {
	case pub.mqQueue <- event:
		return
	default:
	}

	// 队列满，尝试降级：用信号量限制最大并发降级 goroutine 数
	select {
	case pub.fallbackSem <- struct{}{}:
		pub.wg.Add(1) // Fix #5: 纳入 wg，Close 等待此 goroutine 退出
		go func() {
			defer pub.wg.Done()
			defer func() { <-pub.fallbackSem }()
			pub.publishToMQ(ctx, event)
		}()
	default:
		logError("MQ queue and fallback channel both full, event dropped: topic=%s", event.Topic)
	}
}

func (pub *EventPublisher) publishToMQ(ctx context.Context, event *Event) error {
	msg := amqpclt.Message{
		Event:     event.Topic,
		Payload:   event.Payload,
		Timestamp: time.Now(),
	}
	err := pub.amqpPub.Publish(ctx, event.Topic, event.Id, msg)
	pub.memoryBus.metrics.IncMQPublished(event.Topic, err == nil)
	if err != nil {
		logError("failed to publish to MQ: topic=%s error=%v", event.Topic, err)
	} else {
		logInfo("event published to MQ: topic=%s eventId=%s", event.Topic, event.Id)
	}
	return err
}

// startMQWorker 后台 worker：从 mqQueue 取事件，串行推送到 MQ。
func (pub *EventPublisher) startMQWorker() {
	for ev := range pub.mqQueue {
		if ev == nil {
			continue
		}
		if err := pub.publishToMQ(context.Background(), ev); err != nil {
			logError("MQ worker publish failed: %v", err)
		}
	}
}

// Close 关闭 mqQueue 并等待后台 mqWorker 及所有降级 goroutine 全部退出。
//
// Fix #5: 原实现仅等待 mqWorker（wg 只有 1 个计数），降级 goroutine 不在其中，
// 可能在 amqpPub 关闭后仍在运行。现在所有降级 goroutine 均通过 wg.Add(1) 注册，
// wg.Wait() 保证全部退出后才返回。
func (pub *EventPublisher) Close() error {
	if pub.mqQueue != nil {
		close(pub.mqQueue)
		pub.wg.Wait() // 等待 mqWorker + 所有降级 goroutine
	}
	return nil
}

// ============ 便捷简写方法 ============

// Local 发布到本地内存总线（等价于 Publish(..., WithScope(ScopeLocal))）。
func (pub *EventPublisher) Local(ctx context.Context, topic string, payload any) error {
	return pub.Publish(ctx, topic, payload, WithScope(ScopeLocal))
}

// Distributed 发布到 MQ 和本地内存（等价于 Publish(..., WithScope(ScopeDistributed))）。
func (pub *EventPublisher) Distributed(ctx context.Context, topic string, payload any) error {
	return pub.Publish(ctx, topic, payload, WithScope(ScopeDistributed))
}

// MQOnly 仅发布到 MQ（等价于 Publish(..., WithScope(ScopeMQOnly))）。
func (pub *EventPublisher) MQOnly(ctx context.Context, topic string, payload any) error {
	return pub.Publish(ctx, topic, payload, WithScope(ScopeMQOnly))
}
