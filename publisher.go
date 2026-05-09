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
	memoryBus   *EventBus        // 内存事件总线
	amqpPub     *amqpclt.Publish // AMQP 发布者（可选）
	mqQueue     chan *Event      // 本地队列，由后台 worker 负责推送到 MQ
	fallbackSem chan struct{}    // 降级 goroutine 信号量，限制并发数
	wg          sync.WaitGroup
}

// NewEventPublisher 创建事件发布管理器
func NewEventPublisher(memoryBus *EventBus, amqpPub *amqpclt.Publish) *EventPublisher {
	pub := &EventPublisher{
		memoryBus: memoryBus,
		amqpPub:   amqpPub,
	}
	if amqpPub != nil {
		// 使用默认队列大小
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

// Publish 是统一的事件发布方法
func (pub *EventPublisher) Publish(ctx context.Context, topic string, payload any, opts ...func(*PublishOptions)) error {
	options := &PublishOptions{
		Scope: ScopeLocal, // 默认为本地发布
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
		logInfo("[发布] 事件已发布到内存总线, topic=%s, eventId=%s", topic, event.Id)

	case ScopeDistributed:
		if pub.amqpPub == nil {
			logWarn("[发布] 未配置 AMQP，分布式作用域降级为仅本地发布")
			return pub.Publish(ctx, topic, payload, WithScope(ScopeLocal))
		}
		event.Source = "distributed"
		// 把事件放入 MQ 队列，由后台 worker 负责发送，避免为每次发布起 goroutine
		select {
		case pub.mqQueue <- event:
		default:
			// 队列满时降级为直接异步发送，用信号量限制并发 goroutine 数量
			select {
			case pub.fallbackSem <- struct{}{}:
				go func() {
					defer func() { <-pub.fallbackSem }()
					pub.publishToMQ(context.Background(), event)
				}()
			default:
				logError("[发布] MQ队列和降级通道均已满，事件丢弃 topic=%s", topic)
			}
		}
		// 发送到内存总线
		pub.memoryBus.Publish(ctx, event)
		pub.memoryBus.metrics.IncPublished(topic)
		logInfo("[发布] 事件已入队MQ并发布到内存总线, topic=%s, eventId=%s", topic, event.Id)

	case ScopeMQOnly:
		if pub.amqpPub == nil {

			return fmt.Errorf("[发布] 未配置 AMQP 发布者，无法仅发布到 MQ")
		}
		event.Source = "amqp"
		select {
		case pub.mqQueue <- event:
		default:
			// 队列满时降级，用信号量限制并发 goroutine 数量
			select {
			case pub.fallbackSem <- struct{}{}:
				go func() {
					defer func() { <-pub.fallbackSem }()
					pub.publishToMQ(context.Background(), event)
				}()
			default:
				logError("[Publish] MQ 队列满且降级通道也满，丢弃事件 topic=%s", topic)
			}
		}

	default:
		err = fmt.Errorf("未知的发布作用域: %v", options.Scope)
	}

	return err
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
		logError("[发布] 发送到 MQ 失败, topic=%s, error=%v", event.Topic, err)
	} else {
		logInfo("[发布] 事件已发送到 MQ, topic=%s, eventId=%s", event.Topic, event.Id)
	}
	return err
}

// startMQWorker 后台 worker：从 mqQueue 取事件，串行推送到 MQ。
func (pub *EventPublisher) startMQWorker() {
	for ev := range pub.mqQueue {
		if ev == nil {
			continue
		}
		// 使用背景上下文，不应阻塞主业务流程
		if err := pub.publishToMQ(context.Background(), ev); err != nil {
			logError("[MQWorker] 发布失败: %v", err)
		}
	}
}

// Close 关闭 mqQueue 并等待后台 worker 退出。
func (pub *EventPublisher) Close() error {
	if pub.mqQueue != nil {
		close(pub.mqQueue)
		pub.wg.Wait()
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
