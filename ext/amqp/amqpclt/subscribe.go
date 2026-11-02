package amqpclt

import (
	"context"
	"fmt"
	"strings"

	"github.com/bytedance/sonic"
	amqp "github.com/rabbitmq/amqp091-go"
)

var _ Subscriber = (*Subscribe)(nil)

// Delivery 是带手动确认能力的解码消息。
//
// 调用方必须在处理完成后调用 Ack，或按失败策略调用 Nack。未确认的消息在
// channel/连接关闭时由 broker 重新入队；库不会替调用方自动确认。
type Delivery struct {
	Message *Message
	Ack     func() error
	Nack    func(requeue bool) error
}

// Subscribe 实现 AMQP 订阅者。
type Subscribe struct {
	conn        *amqp.Connection
	exchange    string
	queue       string // 消费队列名（不含前缀），必填
	queuePrefix string // 可选：环境/命名空间前缀，格式为 <prefix>_<queue>
	qos         int    // 单 channel 的 prefetch，默认 3
	dlx         string // 死信交换机，空表示不启用
}

// SubscribeOption 订阅者配置项。
type SubscribeOption func(*Subscribe)

// WithQueueName 指定消费队列名（不含前缀），必填。
//
// 本包不提供默认队列名：不同服务共用同一个队列会变成竞争消费，
// 每个服务/实例应按自己的语义显式命名（例如 order-service.events）。
func WithQueueName(name string) SubscribeOption {
	return func(s *Subscribe) { s.queue = strings.TrimSpace(name) }
}

// WithQueuePrefix 为队列名增加环境/命名空间前缀，最终队列名为
// <prefix>_<queue>；prefix 为空时不加前缀。
func WithQueuePrefix(prefix string) SubscribeOption {
	return func(s *Subscribe) { s.queuePrefix = strings.TrimSpace(prefix) }
}

// WithQoS 设置单 channel 的 prefetch（未确认消息上限），默认 3。
//
// QoS 不是 QPS 限流：它限制的是单个消费者同时未确认的消息数。
// 重任务建议设为 1；允许 0（表示不限制），负数会返回错误。
func WithQoS(prefetch int) SubscribeOption {
	return func(s *Subscribe) { s.qos = prefetch }
}

// WithDeadLetterExchange 启用死信队列，指定死信交换机名称。
// 订阅的队列会自动绑定 x-dead-letter-exchange 参数。
func WithDeadLetterExchange(dlx string) SubscribeOption {
	return func(s *Subscribe) { s.dlx = dlx }
}

// NewSubscribe 创建 AMQP 订阅者。
func NewSubscribe(conn *amqp.Connection, exchange string, opts ...SubscribeOption) (*Subscribe, error) {
	s := &Subscribe{
		conn:     conn,
		exchange: exchange,
		qos:      3,
	}
	for _, opt := range opts {
		opt(s)
	}
	if strings.TrimSpace(s.queue) == "" {
		return nil, fmt.Errorf("queue name is required: use WithQueueName")
	}
	if s.qos < 0 {
		return nil, fmt.Errorf("qos must be >= 0")
	}
	return s, nil
}

// fullQueueName 返回加上可选前缀后的实际队列名。
func (s *Subscribe) fullQueueName() string {
	queue := strings.TrimSpace(s.queue)
	prefix := strings.TrimSpace(s.queuePrefix)
	if prefix == "" {
		return queue
	}
	return prefix + "_" + queue
}

// SubscribeRaw 订阅并返回原始 amqp.Delivery 通道。
func (s *Subscribe) SubscribeRaw(ctx context.Context) (<-chan amqp.Delivery, func(), error) {
	if strings.TrimSpace(s.queue) == "" {
		return nil, func() {}, fmt.Errorf("queue name is required: use WithQueueName")
	}
	queueName := s.fullQueueName()

	ch, err := s.conn.Channel()
	if err != nil {
		return nil, func() {}, fmt.Errorf("cannot allocate channel: %v", err)
	}

	cleanUp := func() {
		if err := ch.Close(); err != nil {
			logError(ctx, "cannot close channel: %v", err)
		}
	}

	if err := ch.Qos(s.qos, 0, false); err != nil {
		return nil, cleanUp, fmt.Errorf("cannot set qos: %v", err)
	}

	if err = declareExchange(ch, s.exchange); err != nil {
		return nil, cleanUp, fmt.Errorf("cannot declare exchange: %v", err)
	}

	// 使用调用方显式传入的队列名；本包不再派生默认队列名。
	queueArgs := make(amqp.Table)

	// DLX 配置
	if s.dlx != "" {
		if err := declareExchange(ch, s.dlx); err != nil {
			return nil, cleanUp, fmt.Errorf("cannot declare DLX: %v", err)
		}
		queueArgs["x-dead-letter-exchange"] = s.dlx
	}

	q, err := ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		queueArgs,
	)
	if err != nil {
		return nil, cleanUp, fmt.Errorf("cannot declare queue: %v", err)
	}

	err = ch.QueueBind(q.Name, "", s.exchange, false, nil)
	if err != nil {
		return nil, cleanUp, fmt.Errorf("cannot bind: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",    // consumer tag
		false, // autoAck: false — 手动确认
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		return nil, cleanUp, fmt.Errorf("cannot consume queue: %v", err)
	}
	return msgs, cleanUp, nil
}

var payloadTypeRegistry = make(map[string]func() interface{})

// RegisterPayloadType 注册 Payload 类型构造器，供反序列化时恢复类型信息。
func RegisterPayloadType(payloadType string, payloadFunc func() interface{}) {
	payloadTypeRegistry[payloadType] = payloadFunc
}

// SubscribeManual 订阅并返回解码后的 Delivery 通道，不会自动 Ack。
//
// 调用方处理完消息后必须调用 Delivery.Ack 或 Delivery.Nack；否则消息在
// channel 关闭后会被 broker 重新投递。ctx 取消只会停止转发，未确认消息由
// broker 负责重新入队。
func (s *Subscribe) SubscribeManual(ctx context.Context) (<-chan *Delivery, func(), error) {
	msgCh, cleanUp, err := s.SubscribeRaw(ctx)
	if err != nil {
		return nil, cleanUp, err
	}

	outCh := make(chan *Delivery)
	go func() {
		defer close(outCh)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}

				msgData, err := decodeMessage(ctx, msg)
				if err != nil {
					logError(ctx, "cannot unmarshal message body: %v", err)
					if nackErr := msg.Nack(false, false); nackErr != nil {
						logError(ctx, "failed to nack message: %v", nackErr)
					}
					continue
				}

				// 拷贝 delivery，避免闭包捕获到后续迭代的 msg。
				delivery := msg
				d := &Delivery{
					Message: msgData,
					Ack:     func() error { return delivery.Ack(false) },
					Nack:    func(requeue bool) error { return delivery.Nack(false, requeue) },
				}

				select {
				case outCh <- d:
				case <-ctx.Done():
					// 未确认；关闭 channel 后 broker 会重新入队。
					return
				}
			}
		}
	}()
	return outCh, cleanUp, nil
}

// Subscribe 订阅并返回解码后的 Message 通道，兼容旧的自动确认语义。
//
// 消息写入 outCh 后立即 Ack；这仍然是“交接即确认”，不代表业务处理完成。
// 需要处理完成后确认的场景请使用 SubscribeManual。
func (s *Subscribe) Subscribe(ctx context.Context) (chan *Message, func(), error) {
	deliveries, cleanUp, err := s.SubscribeManual(ctx)
	if err != nil {
		return nil, cleanUp, err
	}

	outCh := make(chan *Message)
	go func() {
		defer close(outCh)
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				select {
				case outCh <- d.Message:
					if ackErr := d.Ack(); ackErr != nil {
						logError(ctx, "failed to ack message: %v", ackErr)
					}
				case <-ctx.Done():
					if nackErr := d.Nack(true); nackErr != nil {
						logError(ctx, "failed to nack message on context done: %v", nackErr)
					}
					return
				}
			}
		}
	}()
	return outCh, cleanUp, nil
}

// decodeMessage 解码消息体，并在 body 未携带 correlationId 时回退到 AMQP 信封。
func decodeMessage(ctx context.Context, msg amqp.Delivery) (*Message, error) {
	var msgData Message
	if err := sonic.Unmarshal(msg.Body, &msgData); err != nil {
		return nil, err
	}
	if msgData.CorrelationId == "" {
		msgData.CorrelationId = msg.CorrelationId
	}

	if msgData.PayloadType != "" {
		if payloadFunc, ok := payloadTypeRegistry[msgData.PayloadType]; ok {
			typed := payloadFunc()
			if raw, ok := msgData.Payload.(map[string]interface{}); ok {
				if b, err := sonic.Marshal(raw); err == nil {
					if err := sonic.Unmarshal(b, typed); err != nil {
						logError(ctx, "cannot restore payload type %s: %v", msgData.PayloadType, err)
					}
					msgData.Payload = typed
				}
			}
		}
	}
	return &msgData, nil
}

// declareExchange 声明 fanout 持久化交换机。
func declareExchange(ch *amqp.Channel, exchange string) error {
	return ch.ExchangeDeclare(
		exchange,
		"fanout",
		true,  // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,
	)
}
