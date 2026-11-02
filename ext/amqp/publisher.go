// Package amqp 提供 RabbitMQ ↔ EventBus 的双向桥接，作为 eventbus 核心包的可选扩展。
//
// 职责单一：
//   - Listener：MQ → 内存总线（单向消费，标记 Source="amqp"）
//   - Publisher：内存总线事件 → MQ（支持 Local / Distributed / MQOnly 三种作用域）
package amqp

import (
	"context"
	"sync"

	"github.com/kcersing/eventbus"

	"github.com/kcersing/eventbus/ext/amqp/amqpclt"
)

// manualSubscriber 是 Listener 依赖的最小订阅能力，便于测试注入 fake。
type manualSubscriber interface {
	SubscribeManual(ctx context.Context) (<-chan *amqpclt.Delivery, func(), error)
}

// ListenerOption 配置 Listener。
type ListenerOption func(*Listener)

// WithDeliveryRecorder 配置投递回执记录器。
//
// 配置后，Listener 只有在 bus.PublishReport 成功且 Record 返回 nil 时才 Ack；
// 记录失败会 Nack 并重投，保证“Ack 不早于投递记录”。未配置时退化为
// “总线接收即 Ack”的投递确认语义。
func WithDeliveryRecorder(r eventbus.DeliveryRecorder) ListenerOption {
	return func(l *Listener) { l.recorder = r }
}

// Listener 监听 RabbitMQ，将消息转换为 Event 后发布到本地 EventBus。
// 不拦截本地事件，不回发到 MQ，避免消息循环。
//
// 默认语义是投递确认：消息交给总线并记录回执后 Ack，不等待业务 Handler 完成。
// 处理完成状态必须由订阅方另行回执。
type Listener struct {
	bus        *eventbus.EventBus
	subscriber manualSubscriber
	recorder   eventbus.DeliveryRecorder
	cancel     context.CancelFunc
	done       chan struct{}
}

// NewListener 创建 AMQP 监听器。
func NewListener(bus *eventbus.EventBus, subscriber *amqpclt.Subscribe, opts ...ListenerOption) *Listener {
	return newListener(bus, subscriber, opts...)
}

// newListener 是 NewListener 的内部实现，接收接口以便测试注入 fake。
func newListener(bus *eventbus.EventBus, subscriber manualSubscriber, opts ...ListenerOption) *Listener {
	l := &Listener{
		bus:        bus,
		subscriber: subscriber,
		done:       make(chan struct{}),
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Start 启动监听，从 RabbitMQ 消费消息并转发到本地 EventBus。
func (l *Listener) Start(ctx context.Context) error {
	if l.recorder == nil {
		logWarn("amqp listener: no delivery recorder configured; ack after bus publish without receipt record")
	}
	ctx, l.cancel = context.WithCancel(ctx)
	go func() {
		defer close(l.done)

		deliveries, cleanup, err := l.subscriber.SubscribeManual(ctx)
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
			case delivery, ok := <-deliveries:
				if !ok {
					logWarn("amqp listener: delivery channel closed")
					return
				}
				l.handle(ctx, delivery)
			}
		}
	}()
	return nil
}

// handle 将一条 MQ delivery 投递到本地总线，记录回执后再确认。
func (l *Listener) handle(ctx context.Context, delivery *amqpclt.Delivery) {
	msg := delivery.Message
	if msg == nil {
		if err := delivery.Nack(false); err != nil {
			logError("amqp listener: nack empty delivery: %v", err)
		}
		return
	}

	event := &eventbus.Event{
		Id:        msg.CorrelationId,
		Topic:     msg.Event,
		Payload:   msg.Payload,
		Timestamp: msg.Timestamp,
		Source:    "amqp",
	}
	if event.Id == "" {
		// 兜底：避免服务端拿空 ID 做幂等/追踪。
		event.Id = eventbus.NewEvent(event.Topic, event.Payload).Id
		logWarn("amqp listener: message without correlation id, generated event id=%s", event.Id)
	}

	receipts, err := l.bus.PublishReport(ctx, event)
	if err != nil {
		logError("amqp listener: publish failed: topic=%s id=%s error=%v", event.Topic, event.Id, err)
		if nackErr := delivery.Nack(true); nackErr != nil {
			logError("amqp listener: failed to nack after publish error: %v", nackErr)
		}
		return
	}

	if l.recorder != nil {
		if err := l.recorder.Record(ctx, receipts); err != nil {
			logError("amqp listener: record receipts failed: topic=%s id=%s error=%v", event.Topic, event.Id, err)
			// 记录失败不确认，交给 broker 重投；要求 Recorder 写操作幂等。
			if nackErr := delivery.Nack(true); nackErr != nil {
				logError("amqp listener: failed to nack after record error: %v", nackErr)
			}
			return
		}
	}

	if err := delivery.Ack(); err != nil {
		logError("amqp listener: failed to ack: topic=%s id=%s error=%v", event.Topic, event.Id, err)
		return
	}
	logInfo("amqp listener: delivered: topic=%s id=%s subscribers=%d", event.Topic, event.Id, len(receipts))
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

// mqPublisher 是 Publisher 依赖的最小 MQ 发布能力，便于测试注入 fake。
type mqPublisher interface {
	Publish(ctx context.Context, routingKey, correlationId string, m amqpclt.Message) error
}

// Publisher 统一事件发布入口，根据 Scope 将事件路由到内存总线和/或 MQ。
// 内部通过队列 + 后台 worker 异步推送 MQ，队列满时通过信号量限制降级并发数。
//
// Close 之后 MQ 发布会被丢弃并记录 Warn 日志，本地发布（Local，以及 Distributed
// 的总线部分）仍可继续使用；Close 幂等，且可与其他方法并发调用。
type Publisher struct {
	bus         *eventbus.EventBus
	amqpPub     mqPublisher
	mqQueue     chan *eventbus.Event
	fallbackSem chan struct{}
	wg          sync.WaitGroup

	mu        sync.RWMutex // 保护 closed 以及 enqueue 与 Close 的时序
	closed    bool
	closeOnce sync.Once
}

// NewPublisher 创建事件发布器。amqpPub 为 nil 时退化为纯本地发布器。
func NewPublisher(bus *eventbus.EventBus, amqpPub *amqpclt.Publish) *Publisher {
	var mq mqPublisher
	if amqpPub != nil { // 注意：nil *Publish 直接转成接口不是 nil，必须先判空
		mq = amqpPub
	}
	return newPublisher(bus, mq)
}

// newPublisher 是 NewPublisher 的内部实现，接收接口以便测试注入 fake。
func newPublisher(bus *eventbus.EventBus, amqpPub mqPublisher) *Publisher {
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

// Close 关闭 Publisher：拒绝新的 MQ 发布、关闭内部队列，并等待在途发送与降级
// goroutine 全部退出。可重复调用，也可与发布方法并发调用。
func (p *Publisher) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		if p.mqQueue != nil {
			close(p.mqQueue)
		}
		p.mu.Unlock()
		p.wg.Wait()
	})
	return nil
}

// enqueue 将事件放入 MQ 队列；队列满时通过信号量限制的降级 goroutine 直接发送。
// 与 Close 的时序由 p.mu 保护：Close 持写锁时不会再有入队或 wg.Add，避免向已关闭
// 的 channel 发送，也避免 WaitGroup 的 Add/Wait 误用。
func (p *Publisher) enqueue(ctx context.Context, event *eventbus.Event) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		logWarn("publisher: closed, event dropped: topic=%s", event.Topic)
		return
	}

	select {
	case p.mqQueue <- event:
		return
	default:
	}
	select {
	case p.fallbackSem <- struct{}{}:
		p.wg.Add(1) // 持读锁期间 Add，Close 的 Wait 一定发生在所有 Add 之后
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
		CorrelationId: event.Id,
		Event:         event.Topic,
		Payload:       event.Payload,
		Timestamp:     event.Timestamp,
	})
	if err != nil {
		logError("publisher: MQ publish failed: topic=%s error=%v", event.Topic, err)
	} else {
		logInfo("publisher: MQ publish ok: topic=%s id=%s", event.Topic, event.Id)
	}
}
