// Package main 演示 RabbitMQ 与内存总线的双向桥接。
//
// 需要先启动 RabbitMQ，并通过环境变量配置：
//
//	AMQP_URL      默认 amqp://guest:guest@localhost:5672/
//	AMQP_EXCHANGE 默认 eventbus.demo
//	AMQP_QUEUE    默认 <exchange>.events（必须显式指定，库不再派生队列名）
//
// 注意：Listener 的 Ack 语义是“投递确认”：消息交给总线并记录投递回执后
// 立即 Ack，不会等待业务 Handler 处理完成。处理失败由订阅方自行补偿。
// 详见 docs/architecture.md。
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/kcersing/eventbus"
	eventamqp "github.com/kcersing/eventbus/ext/amqp"
	"github.com/kcersing/eventbus/ext/amqp/amqpclt"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

func main() {
	url := getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
	exchange := getenv("AMQP_EXCHANGE", "eventbus.demo")
	queue := getenv("AMQP_QUEUE", exchange+".events")

	conn, err := amqp091.Dial(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect rabbitmq: %v\n", err)
		os.Exit(1)
	}

	// 提前声明死信交换机/队列：WithDeadLetterExchange 只声明 exchange，
	// 不会自动创建 DLQ，因此这里显式调用 DeclareDLX。
	dlxCh, err := conn.Channel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "open channel: %v\n", err)
		os.Exit(1)
	}
	if err := amqpclt.DeclareDLX(dlxCh, exchange); err != nil {
		fmt.Fprintf(os.Stderr, "declare dlx: %v\n", err)
		os.Exit(1)
	}
	_ = dlxCh.Close()

	amqpPub, err := amqpclt.NewPublisher(conn, exchange)
	if err != nil {
		fmt.Fprintf(os.Stderr, "new amqp publisher: %v\n", err)
		os.Exit(1)
	}
	amqpSub, err := amqpclt.NewSubscribe(conn, exchange,
		amqpclt.WithQueueName(queue),
		amqpclt.WithDeadLetterExchange(exchange+".dlx"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "new amqp subscriber: %v\n", err)
		os.Exit(1)
	}

	bus := eventbus.New()
	recorder := logRecorder{}
	listener := eventamqp.NewListener(bus, amqpSub, eventamqp.WithDeliveryRecorder(recorder))
	publisher := eventamqp.NewPublisher(bus, amqpPub)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := listener.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "start listener: %v\n", err)
		os.Exit(1)
	}

	// 本地订阅者：既会收到 Local 发布，也会收到从 MQ 回环转发的事件。
	localSub := bus.SubscribeAsync(ctx, "order.created",
		eventbus.HandlerFunc(func(ctx context.Context, e *eventbus.Event) error {
			fmt.Printf("[bus] topic=%s source=%s payload=%v\n", e.Topic, e.Source, e.Payload)
			return nil
		}), 2)
	defer localSub.Unsubscribe()

	publisher.Local(ctx, "order.created", map[string]any{"id": 1})
	if err := publisher.MQOnly(ctx, "order.created", map[string]any{"id": 2}); err != nil {
		fmt.Fprintf(os.Stderr, "mq-only publish: %v\n", err)
	}
	if err := publisher.Distributed(ctx, "order.created", map[string]any{"id": 3}); err != nil {
		fmt.Fprintf(os.Stderr, "distributed publish: %v\n", err)
	}

	// 等待 Ctrl+C；未按 Ctrl+C 时 3 秒后退出，便于脚本演示。
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
	}

	listener.Stop()
	_ = publisher.Close()
	_ = bus.Close()
	_ = conn.Close()
	fmt.Println("done")
}

// logRecorder 打印投递回执；生产环境应替换为持久化 Recorder。
// Listener 只有 Record 返回 nil 才会 Ack。
type logRecorder struct{}

func (logRecorder) Record(_ context.Context, receipts []eventbus.DeliveryReceipt) error {
	for _, r := range receipts {
		fmt.Printf("[receipt] event=%s subscriber=%s topic=%s status=%s\n",
			r.EventID, r.SubscriberID, r.SubscriptionTopic, r.Status)
	}
	return nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
