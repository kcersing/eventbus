package amqpclt

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	amqp "github.com/rabbitmq/amqp091-go"
)

// integrationConn 在设置 AMQP_URL 时连接真实 RabbitMQ，否则跳过集成测试。
func integrationConn(t *testing.T) *amqp.Connection {
	t.Helper()
	url := os.Getenv("AMQP_URL")
	if url == "" {
		t.Skip("AMQP_URL not set; skipping RabbitMQ integration test")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func uniqueBase() string {
	return fmt.Sprintf("eventbus.it.%d", time.Now().UnixNano())
}

// cleanupResources 删除测试创建的队列与交换机；应在连接关闭前执行。
func cleanupResources(t *testing.T, conn *amqp.Connection, queues, exchanges []string) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Logf("cleanup: open channel: %v", err)
		return
	}
	defer ch.Close()

	for _, q := range queues {
		if _, err := ch.QueueDelete(q, false, false, false); err != nil {
			t.Logf("cleanup: delete queue %s: %v", q, err)
		}
	}
	for _, ex := range exchanges {
		if err := ch.ExchangeDelete(ex, false, false); err != nil {
			t.Logf("cleanup: delete exchange %s: %v", ex, err)
		}
	}
}

// Publish → SubscribeManual 的真实链路：消息可被解码、可手动 Ack。
func TestIntegrationPublishSubscribeManual(t *testing.T) {
	conn := integrationConn(t)
	base := uniqueBase()
	exchange, queue := base+".exchange", base+".queue"
	t.Cleanup(func() { cleanupResources(t, conn, []string{queue}, []string{exchange}) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sub, err := NewSubscribe(conn, exchange, WithQueueName(queue), WithQoS(1))
	if err != nil {
		t.Fatalf("NewSubscribe: %v", err)
	}
	deliveries, cleanup, err := sub.SubscribeManual(ctx)
	if err != nil {
		t.Fatalf("SubscribeManual: %v", err)
	}
	defer cleanup()

	pub, err := NewPublisher(conn, exchange)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish(ctx, "", "corr-integration", Message{
		Event:   "order.created",
		Payload: map[string]any{"id": 7},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case d, ok := <-deliveries:
		if !ok {
			t.Fatal("deliveries channel closed")
		}
		if d.Message.Event != "order.created" {
			t.Fatalf("event = %q, want order.created", d.Message.Event)
		}
		if d.Message.CorrelationId != "corr-integration" {
			t.Fatalf("correlation id = %q, want corr-integration", d.Message.CorrelationId)
		}
		payload, ok := d.Message.Payload.(map[string]interface{})
		if !ok {
			t.Fatalf("payload type = %T, want map", d.Message.Payload)
		}
		if got, ok := payload["id"].(float64); !ok || got != 7 {
			t.Fatalf("payload id = %v (%T), want 7", payload["id"], payload["id"])
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for delivery")
	}
}

// queueMessageCount 返回队列中待消费（ready）的消息数。
func queueMessageCount(t *testing.T, conn *amqp.Connection, queue string) int {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer ch.Close()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclarePassive: %v", err)
	}
	return q.Messages
}

// waitQueueMessages 轮询等待队列消息数达到期望值。
func waitQueueMessages(t *testing.T, conn *amqp.Connection, queue string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if queueMessageCount(t, conn, queue) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("queue %s messages = %d, want %d", queue, queueMessageCount(t, conn, queue), want)
}

// 发布者关闭后，单条/批量/异步发布都应返回错误而不是 panic。
func TestIntegrationPublishAfterClose(t *testing.T) {
	conn := integrationConn(t)
	base := uniqueBase()
	exchange := base + ".exchange"
	t.Cleanup(func() { cleanupResources(t, conn, nil, []string{exchange}) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pub, err := NewPublisher(conn, exchange, WithMaxRetries(0))
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	pub.Close()

	if err := pub.Publish(ctx, "", "corr-closed", Message{Event: "order.created", Payload: "x"}); err == nil {
		t.Fatal("expected error when publishing after Close")
	}

	failed, err := pub.PublishBatch(ctx, []BatchMessage{{
		CorrelationId: "corr-closed",
		Message:       Message{Event: "order.created", Payload: "x"},
	}})
	if err != nil {
		t.Fatalf("PublishBatch overall error: %v", err)
	}
	if len(failed) != 1 || failed[0] != 0 {
		t.Fatalf("failed indices = %v, want [0]", failed)
	}

	results := make(chan PublishResult, 1)
	pub.PublishAsync(ctx, []BatchMessage{{
		CorrelationId: "corr-closed",
		Message:       Message{Event: "order.created", Payload: "x"},
	}}, results)

	res, ok := <-results
	if !ok {
		t.Fatal("expected one async result")
	}
	if res.Error == nil {
		t.Fatal("expected async publish error after Close")
	}
	if _, ok := <-results; ok {
		t.Fatal("expected closed result channel")
	}
}

// 未确认消息在 channel 关闭后应由 broker 重新入队。
func TestIntegrationUnackedMessageRequeuedOnCancel(t *testing.T) {
	conn := integrationConn(t)
	base := uniqueBase()
	exchange, queue := base+".exchange", base+".queue"
	t.Cleanup(func() { cleanupResources(t, conn, []string{queue}, []string{exchange}) })

	sub, err := NewSubscribe(conn, exchange, WithQueueName(queue), WithQoS(1))
	if err != nil {
		t.Fatalf("NewSubscribe: %v", err)
	}

	pub, err := NewPublisher(conn, exchange)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	deliveries1, cleanup1, err := sub.SubscribeManual(ctx1)
	if err != nil {
		cancel1()
		t.Fatalf("SubscribeManual: %v", err)
	}

	if err := pub.Publish(ctx1, "", "corr-requeue", Message{Event: "order.requeue"}); err != nil {
		cancel1()
		cleanup1()
		t.Fatalf("Publish: %v", err)
	}

	select {
	case _, ok := <-deliveries1:
		if !ok {
			cancel1()
			cleanup1()
			t.Fatal("deliveries channel closed")
		}
	case <-time.After(5 * time.Second):
		cancel1()
		cleanup1()
		t.Fatal("timed out waiting first delivery")
	}

	// 不 Ack，取消并关闭 channel，未确认消息由 broker 重新入队。
	cancel1()
	cleanup1()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	deliveries2, cleanup2, err := sub.SubscribeManual(ctx2)
	if err != nil {
		t.Fatalf("second SubscribeManual: %v", err)
	}
	defer cleanup2()

	select {
	case d, ok := <-deliveries2:
		if !ok {
			t.Fatal("second deliveries channel closed")
		}
		if d.Message.Event != "order.requeue" {
			t.Fatalf("requeued event = %q, want order.requeue", d.Message.Event)
		}
		if d.Message.CorrelationId != "corr-requeue" {
			t.Fatalf("requeued correlation id = %q, want corr-requeue", d.Message.CorrelationId)
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack requeued message: %v", err)
		}
	case <-ctx2.Done():
		t.Fatal("timed out waiting requeued delivery")
	}
}

// 无法解码的消息应被 Nack(false) 丢弃，不会进入消费通道。
func TestIntegrationInvalidMessageDiscarded(t *testing.T) {
	conn := integrationConn(t)
	base := uniqueBase()
	exchange, queue := base+".exchange", base+".queue"
	t.Cleanup(func() { cleanupResources(t, conn, []string{queue}, []string{exchange}) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sub, err := NewSubscribe(conn, exchange, WithQueueName(queue), WithQoS(1))
	if err != nil {
		t.Fatalf("NewSubscribe: %v", err)
	}
	deliveries, cleanup, err := sub.SubscribeManual(ctx)
	if err != nil {
		t.Fatalf("SubscribeManual: %v", err)
	}
	defer cleanup()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	err = ch.PublishWithContext(ctx, exchange, "", false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         []byte("not-json"),
		DeliveryMode: amqp.Persistent,
	})
	ch.Close()
	if err != nil {
		t.Fatalf("publish invalid body: %v", err)
	}

	select {
	case d, ok := <-deliveries:
		if ok {
			t.Fatalf("unexpected delivery for invalid body: %+v", d.Message)
		}
	case <-time.After(500 * time.Millisecond):
	}

	// decode 失败后 Nack(false)，消息应被丢弃而不是留在队列。
	waitQueueMessages(t, conn, queue, 0, 2*time.Second)
}

// nil payload 不应导致 Publish panic，也能被对端正常解码。
func TestIntegrationPublishNilPayload(t *testing.T) {
	conn := integrationConn(t)
	base := uniqueBase()
	exchange, queue := base+".exchange", base+".queue"
	t.Cleanup(func() { cleanupResources(t, conn, []string{queue}, []string{exchange}) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sub, err := NewSubscribe(conn, exchange, WithQueueName(queue), WithQoS(1))
	if err != nil {
		t.Fatalf("NewSubscribe: %v", err)
	}
	deliveries, cleanup, err := sub.SubscribeManual(ctx)
	if err != nil {
		t.Fatalf("SubscribeManual: %v", err)
	}
	defer cleanup()

	pub, err := NewPublisher(conn, exchange)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish(ctx, "", "corr-nil", Message{Event: "order.nil", Payload: nil}); err != nil {
		t.Fatalf("Publish nil payload: %v", err)
	}

	select {
	case d, ok := <-deliveries:
		if !ok {
			t.Fatal("deliveries channel closed")
		}
		if d.Message.Event != "order.nil" {
			t.Fatalf("event = %q, want order.nil", d.Message.Event)
		}
		if d.Message.Payload != nil {
			t.Fatalf("payload = %v (%T), want nil", d.Message.Payload, d.Message.Payload)
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for delivery")
	}
}

// Nack(false) 且队列配置 DLX 时，消息应进入死信队列。
func TestIntegrationDeadLetterQueue(t *testing.T) {
	conn := integrationConn(t)
	base := uniqueBase()
	exchange, queue := base+".exchange", base+".queue"
	dlx, dlq := DLXNames(base)
	t.Cleanup(func() {
		cleanupResources(t, conn, []string{queue, dlq}, []string{exchange, dlx})
	})

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	if err := DeclareDLX(ch, base); err != nil {
		ch.Close()
		t.Fatalf("DeclareDLX: %v", err)
	}
	ch.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sub, err := NewSubscribe(conn, exchange,
		WithQueueName(queue),
		WithDeadLetterExchange(dlx),
	)
	if err != nil {
		t.Fatalf("NewSubscribe: %v", err)
	}
	deliveries, cleanup, err := sub.SubscribeManual(ctx)
	if err != nil {
		t.Fatalf("SubscribeManual: %v", err)
	}
	defer cleanup()

	dlxDeliveries, cleanupDLX, err := ConsumeDLX(ctx, conn, base)
	if err != nil {
		t.Fatalf("ConsumeDLX: %v", err)
	}
	defer cleanupDLX()

	pub, err := NewPublisher(conn, exchange)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish(ctx, "", "corr-dlx", Message{
		Event:   "order.failed",
		Payload: "boom",
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case d, ok := <-deliveries:
		if !ok {
			t.Fatal("deliveries channel closed")
		}
		if err := d.Nack(false); err != nil {
			t.Fatalf("Nack: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for delivery")
	}

	select {
	case d, ok := <-dlxDeliveries:
		if !ok {
			t.Fatal("dlx deliveries channel closed")
		}
		var got Message
		if err := sonic.Unmarshal(d.Body, &got); err != nil {
			t.Fatalf("unmarshal dead letter: %v", err)
		}
		if got.Event != "order.failed" {
			t.Fatalf("dead letter event = %q, want order.failed", got.Event)
		}
		if err := d.Ack(false); err != nil {
			t.Fatalf("Ack dead letter: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for dead letter")
	}
}
