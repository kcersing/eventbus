package amqpclt

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DLXNames 返回默认的死信交换机名和死信队列名。
func DLXNames(exchange string) (dlxName, dlqName string) {
	return exchange + ".dlx", exchange + ".dlq"
}

// DeclareDLX 声明死信交换机和死信队列。
// 死信交换机为 fanout 持久化类型，死信队列为持久化队列并绑定到该交换机。
func DeclareDLX(ch *amqp.Channel, exchange string) error {
	dlx, dlq := DLXNames(exchange)

	if err := declareExchange(ch, dlx); err != nil {
		return fmt.Errorf("declare DLX exchange: %w", err)
	}

	if _, err := ch.QueueDeclare(
		dlq,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	); err != nil {
		return fmt.Errorf("declare DLQ: %w", err)
	}

	if err := ch.QueueBind(dlq, "", dlx, false, nil); err != nil {
		return fmt.Errorf("bind DLQ to DLX: %w", err)
	}

	return nil
}

// ConsumeDLX 从死信队列消费消息，返回 amqp.Delivery 通道和清理函数。
// 消息被路由到 DLX 后（如 Nack 不重入队、TTL 过期、队列溢出），可通过此函数消费。
func ConsumeDLX(ctx context.Context, conn *amqp.Connection, exchange string) (<-chan amqp.Delivery, func(), error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, func() {}, fmt.Errorf("DLX consumer: cannot allocate channel: %w", err)
	}

	_, dlq := DLXNames(exchange)

	// 确保 DLX 和死信队列已声明
	if err := DeclareDLX(ch, exchange); err != nil {
		ch.Close()
		return nil, func() {}, fmt.Errorf("DLX consumer: %w", err)
	}

	msgs, err := ch.Consume(
		dlq,
		"",    // consumer tag
		false, // autoAck: false — 手动确认
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		ch.Close()
		return nil, func() {}, fmt.Errorf("DLX consumer: cannot consume DLQ: %w", err)
	}

	cleanup := func() {
		if err := ch.Close(); err != nil {
			// 无法使用 log 回调，静默处理
			_ = err
		}
	}

	return msgs, cleanup, nil
}
