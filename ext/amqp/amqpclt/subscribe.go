package amqpclt

import (
	"context"
	"fmt"

	"github.com/bytedance/sonic"
	amqp "github.com/rabbitmq/amqp091-go"
)

var _ Subscriber = (*Subscribe)(nil)

// Subscribe implements an amqp subscribe.
type Subscribe struct {
	conn     *amqp.Connection // AMQP 连接
	exchange string           // 交换机名称
	log      func(context.Context, ...any)
}

// NewSubscribe creates an amqp subscribe.
func NewSubscribe(conn *amqp.Connection, exchange string) (*Subscribe, error) {
	return &Subscribe{
		conn:     conn,
		exchange: exchange,
	}, nil
}

// SubscribeRaw subscribes and returns a channel with raw amqp delivery.
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

	// 声明一个共享的、持久化的工作队列
	// 这个队列由所有服务实例共享，用于接收分发到该服务的事件。
	// RabbitMQ 会将消息以轮询（round-robin）的方式分发给监听此队列的多个消费者。
	queueName := fmt.Sprintf("%s_events_queue", s.exchange)
	q, err := ch.QueueDeclare(
		queueName, // 使用基于交换机名的固定队列名
		true,      // durable: 持久化，RabbitMQ重启后队列依然存在
		false,     // autoDelete: 当没有消费者时，不自动删除
		false,     // exclusive: 非排他，允许其他消费者连接
		false,     // noWait: 阻塞等待服务器响应
		nil,       // 额外参数
	)
	if err != nil {
		return nil, func() {}, fmt.Errorf("cannot declare queue: %v", err)
	}

	// 清理函数：只关闭通道，不再删除队列
	cleanUp := func() {
		if err := ch.Close(); err != nil {
			s.log(ctx, fmt.Sprintf("cannot close channel %s", err.Error()))
		}
	}

	// 将队列绑定到交换机。对于 fanout 交换机，routing key 通常被忽略。
	err = ch.QueueBind(q.Name, "", s.exchange, false, nil)
	if err != nil {
		return nil, cleanUp, fmt.Errorf("cannot bind: %v", err)
	}

	// 消费队列消息
	// autoAck 设置为 false，启用手动确认模式
	msgs, err := ch.Consume(
		q.Name,
		"",    // consumer tag
		false, // autoAck: false
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return nil, cleanUp, fmt.Errorf("cannot consume queue: %v", err)
	}
	return msgs, cleanUp, nil
}

var payloadTypeRegistry = make(map[string]func() interface{})

func RegisterPayloadType(payloadType string, payloadFunc func() interface{}) {
	payloadTypeRegistry[payloadType] = payloadFunc
}

// Subscribe subscribes and returns a channel with CarEntity data.
func (s *Subscribe) Subscribe(ctx context.Context) (chan *Message, func(), error) {
	msgCh, cleanUp, err := s.SubscribeRaw(ctx)
	if err != nil {
		return nil, cleanUp, err
	}
	carCh := make(chan *Message)
	go func() {
		defer close(carCh) // 确保通道关闭，避免接收方阻塞
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				var msgData Message

				// 反序列化消息
				if err := sonic.Unmarshal(msg.Body, &msgData); err != nil {
					s.log(ctx, fmt.Sprintf("cannot unmarshal message body: %v", err))
					// 反序列化失败：拒绝消息（不重新入队，避免死循环）
					if nackErr := msg.Nack(false, false); nackErr != nil {
						s.log(ctx, fmt.Sprintf("failed to nack message: %v", nackErr))
					}
					continue
				}
				if msgData.PayloadType == "" {
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

				// 发送到业务通道（阻塞直到业务方接收）
				select {
				case carCh <- &msgData:
					// 业务处理完成：手动确认消息
					if ackErr := msg.Ack(false); ackErr != nil {
						s.log(ctx, fmt.Sprintf("failed to ack message: %v", ackErr))
					}
				case <-ctx.Done():
					// 上下文取消，尝试重新入队或 nack
					if nackErr := msg.Nack(false, true); nackErr != nil {
						s.log(ctx, fmt.Sprintf("failed to nack message on context done: %v", nackErr))
					}
					return
				}
			}
		}
	}()
	return carCh, cleanUp, nil
}

// 声明交换机，类型为 fanout（广播模式）
func declareExchange(ch *amqp.Channel, exchange string) error {
	return ch.ExchangeDeclare(
		exchange, // 交换机名称
		"fanout", // 类型：fanout 会将消息广播到所有绑定的队列
		true,     // durable: 交换机是否持久化
		false,    // autoDelete: 当没有队列绑定时是否自动删除
		false,    // internal: 是否为内部交换机
		false,    // noWait: 是否非阻塞
		nil,      // 参数
	)
}
