// Package amqp 提供 RabbitMQ ↔ EventBus 的双向桥接，作为 eventbus 核心包的可选扩展。
//
// 职责单一：
//   - Listener：MQ → 内存总线（单向消费，标记 Source="amqp"）
//   - Publisher：内存总线事件 → MQ（支持 Local / Distributed / MQOnly 三种作用域）
package amqp

import (
	"context"
	"eventbus"
	"sync"
	"time"

	"eventbus/ext/amqp/amqpclt"
)

// Listener 监听 RabbitMQ，将消息转换为 Event 后发布到本地 EventBus。
// 不拦截本地事件，不回发到 MQ，避免消息循环。
type Listener struct {
	bus        *eventbus.EventBus
	subscriber *amqpclt.Subscribe
	cancel     context.CancelFunc
	done       chan struct{}
}

// NewListener 创建 AMQP 监听器。
func NewListener(bus *eventbus.EventBus, subscriber *amqpclt.Subscribe) *Listener {
	return &Listener{
		bus:        bus,
		subscriber: subscriber,
		done:       make(chan struct{}),
	}
}

// Start 启动监听，从 RabbitMQ 消费消息并转发到本地 EventBus。
func (l *Listener) Start(ctx context.Context) error {
	ctx, l.cancel = context.WithCancel(ctx)
	go func() {
		defer close(l.done)

		msgCh, cleanup, err := l.subscriber.Subscribe(ctx)
		if err != nil {
			logError("amqp listener: subscribe failed: %v", err)
			return
		}
		defer cleanup()
		logInfo("amqp listener: started")

		for {
			select {
			case <-ctx.Done():
				logInfo("amqp listener: stopped")
				return
			case msg, ok := <-msgCh:
				if !ok {
					logWarn("amqp listener: message channel closed")
					return
				}
				l.bus.Publish(ctx, &eventbus.Event{
					Id:        msg.CorrelationId,
					Topic:     msg.Event,
					Payload:   msg.Payload,
					Timestamp: msg.Timestamp,
					Source:    "amqp",
				})
				logInfo("amqp listener: forwarded: topic=%s id=%s", msg.Event, msg.CorrelationId)
			}
		}
	}()
	return nil
}

// Stop 停止监听，等待内部 goroutine 退出。
func (l *Listener) Stop() {
	l.cancel()
	<-l.done
}

// ── Publisher ────────────────────────────────────────────────────────────────

// Scope 定义事件的发布范围。
type Scope int

const (
	// ScopeLocal 仅发布到本地内存总线（默认）。
	ScopeLocal Scope = iota + 1
	// ScopeDistributed 同时发布到 MQ 和本地内存总线。
	ScopeDistributed
	// ScopeMQOnly 仅发布到 MQ，本地订阅者不感知。
	ScopeMQOnly
)

// Publisher 统一事件发布入口，根据 Scope 将事件路由到内存总线和/或 MQ。
// 内部通过队列 + 后台 worker 异步推送 MQ，队列满时通过信号量限制降级并发数。
type Publisher struct {
	bus         *eventbus.EventBus
	amqpPub     *amqpclt.Publish
	mqQueue     chan *eventbus.Event
	fallbackSem chan struct{}
	wg          sync.WaitGroup
}

// NewPublisher 创建事件发布器。amqpPub 为 nil 时退化为纯本地发布器。
func NewPublisher(bus *eventbus.EventBus, amqpPub *amqpclt.Publish) *Publisher {
	p := &Publisher{
		bus:     bus,
		amqpPub: amqpPub,
	}
	if amqpPub != nil {
		p.mqQueue = make(chan *eventbus.Event, 1000)
		p.fallbackSem = make(chan struct{}, 100)
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.mqWorker()
		}()
	}
	return p
}

// Local 仅发布到本地内存总线。
func (p *Publisher) Local(ctx context.Context, topic string, payload any) {
	event := eventbus.NewEvent(topic, payload)
	event.Source = "local"
	p.bus.Publish(ctx, event)
	logInfo("publisher: local: topic=%s id=%s", topic, event.Id)
}

// Distributed 同时发布到 MQ 和本地内存总线。
func (p *Publisher) Distributed(ctx context.Context, topic string, payload any) error {
	if p.amqpPub == nil {
		logWarn("publisher: AMQP not configured, fallback to local: topic=%s", topic)
		p.Local(ctx, topic, payload)
		return nil
	}
	event := eventbus.NewEvent(topic, payload)
	event.Source = "distributed"
	p.enqueue(ctx, event)
	p.bus.Publish(ctx, event)
	logInfo("publisher: distributed: topic=%s id=%s", topic, event.Id)
	return nil
}

// MQOnly 仅发布到 MQ，本地订阅者不感知。
func (p *Publisher) MQOnly(ctx context.Context, topic string, payload any) error {
	if p.amqpPub == nil {
		return errNoAMQP
	}
	event := eventbus.NewEvent(topic, payload)
	event.Source = "amqp"
	p.enqueue(ctx, event)
	logInfo("publisher: mq-only: topic=%s id=%s", topic, event.Id)
	return nil
}

// Close 关闭 Publisher，等待后台 worker 和所有降级 goroutine 全部退出。
func (p *Publisher) Close() error {
	if p.mqQueue != nil {
		close(p.mqQueue)
		p.wg.Wait()
	}
	return nil
}

// enqueue 将事件放入 MQ 队列；队列满时通过信号量限制的降级 goroutine 直接发送。
func (p *Publisher) enqueue(ctx context.Context, event *eventbus.Event) {
	select {
	case p.mqQueue <- event:
		return
	default:
	}
	select {
	case p.fallbackSem <- struct{}{}:
		p.wg.Add(1) // 纳入 wg，Close 等待此 goroutine 完成
		go func() {
			defer p.wg.Done()
			defer func() { <-p.fallbackSem }()
			p.sendToMQ(ctx, event)
		}()
	default:
		logError("publisher: MQ queue and fallback both full, event dropped: topic=%s", event.Topic)
	}
}

func (p *Publisher) mqWorker() {
	for ev := range p.mqQueue {
		if ev != nil {
			p.sendToMQ(context.Background(), ev)
		}
	}
}

func (p *Publisher) sendToMQ(ctx context.Context, event *eventbus.Event) {
	err := p.amqpPub.Publish(ctx, event.Topic, event.Id, amqpclt.Message{
		Event:     event.Topic,
		Payload:   event.Payload,
		Timestamp: time.Now(),
	})
	if err != nil {
		logError("publisher: MQ publish failed: topic=%s error=%v", event.Topic, err)
	} else {
		logInfo("publisher: MQ publish ok: topic=%s id=%s", event.Topic, event.Id)
	}
}
