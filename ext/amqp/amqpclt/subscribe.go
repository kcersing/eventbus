package amqpclt

import (
	"context"
	"fmt"

	"github.com/bytedance/sonic"
	amqp "github.com/rabbitmq/amqp091-go"
)

var _ Subscriber = (*Subscribe)(nil)

// Subscribe 实现 AMQP 订阅者。
type Subscribe struct {
	conn     *amqp.Connection
	exchange string
	dlx      string // 死信交换机，空表示不启用
	log      func(context.Context, ...any)
}

// SubscribeOption 订阅者配置项。
type SubscribeOption func(*Subscribe)

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
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// SubscribeRaw 订阅并返回原始 amqp.Delivery 通道。
func (s *Subscribe) SubscribeRaw(ctx context.Context) (<-chan amqp.Delivery, func(), error) {
	ch, err := s.conn.Channel()
	if err != nil {
		return nil, func() {}, fmt.Errorf("cannot allocate channel: %v", err)
	}

	if err := ch.Qos(3, 0, false); err != nil {
		return nil, func() {}, fmt.Errorf("cannot set qos: %v", err)
	}

	if err = declareExchange(ch, s.exchange); err != nil {
		return nil, func() {}, fmt.Errorf("cannot declare exchange: %v", err)
	}

	// 声明共享持久化工作队列
	queueName := fmt.Sprintf("%s_events_queue", s.exchange)
	queueArgs := make(amqp.Table)

	// DLX 配置
	if s.dlx != "" {
		if err := declareExchange(ch, s.dlx); err != nil {
			return nil, func() {}, fmt.Errorf("cannot declare DLX: %v", err)
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
		return nil, func() {}, fmt.Errorf("cannot declare queue: %v", err)
	}

	cleanUp := func() {
		if err := ch.Close(); err != nil {
			s.log(ctx, fmt.Sprintf("cannot close channel %s", err.Error()))
		}
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

// Subscribe 订阅并返回解码后的 Message 通道。
func (s *Subscribe) Subscribe(ctx context.Context) (chan *Message, func(), error) {
	msgCh, cleanUp, err := s.SubscribeRaw(ctx)
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
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				var msgData Message

				if err := sonic.Unmarshal(msg.Body, &msgData); err != nil {
					s.log(ctx, fmt.Sprintf("cannot unmarshal message body: %v", err))
					if nackErr := msg.Nack(false, false); nackErr != nil {
						s.log(ctx, fmt.Sprintf("failed to nack message: %v", nackErr))
					}
					continue
				}

				if msgData.PayloadType != "" {
					if payloadFunc, ok := payloadTypeRegistry[msgData.PayloadType]; ok {
						typed := payloadFunc()
						if raw, ok := msgData.Payload.(map[string]interface{}); ok {
							if b, err := sonic.Marshal(raw); err == nil {
								sonic.Unmarshal(b, typed)
								msgData.Payload = typed
							}
						}
					}
				}

				select {
				case outCh <- &msgData:
					if ackErr := msg.Ack(false); ackErr != nil {
						s.log(ctx, fmt.Sprintf("failed to ack message: %v", ackErr))
					}
				case <-ctx.Done():
					if nackErr := msg.Nack(false, true); nackErr != nil {
						s.log(ctx, fmt.Sprintf("failed to nack message on context done: %v", nackErr))
					}
					return
				}
			}
		}
	}()
	return outCh, cleanUp, nil
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
