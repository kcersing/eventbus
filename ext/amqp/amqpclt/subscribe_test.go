package amqpclt

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDecodeMessageCorrelationFallback(t *testing.T) {
	body, err := sonic.Marshal(Message{Event: "order.created", Payload: "x"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	msg, err := decodeMessage(context.Background(), amqp.Delivery{
		Body:          body,
		CorrelationId: "envelope-id",
	})
	if err != nil {
		t.Fatalf("decodeMessage: %v", err)
	}
	if msg.CorrelationId != "envelope-id" {
		t.Fatalf("CorrelationId = %q, want envelope-id", msg.CorrelationId)
	}
}

func TestDecodeMessagePrefersBodyCorrelation(t *testing.T) {
	body, err := sonic.Marshal(Message{CorrelationId: "body-id", Event: "order.created"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	msg, err := decodeMessage(context.Background(), amqp.Delivery{
		Body:          body,
		CorrelationId: "envelope-id",
	})
	if err != nil {
		t.Fatalf("decodeMessage: %v", err)
	}
	if msg.CorrelationId != "body-id" {
		t.Fatalf("CorrelationId = %q, want body-id", msg.CorrelationId)
	}
}

func TestDecodeMessageInvalidJSON(t *testing.T) {
	_, err := decodeMessage(context.Background(), amqp.Delivery{Body: []byte("not-json")})
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestNewSubscribeRequiresQueueName(t *testing.T) {
	if _, err := NewSubscribe(nil, "eventbus.demo"); err == nil {
		t.Fatal("expected error when queue name is missing")
	}
}

func TestNewSubscribeWithQueueName(t *testing.T) {
	s, err := NewSubscribe(nil, "eventbus.demo", WithQueueName("  order-service.events  "))
	if err != nil {
		t.Fatalf("NewSubscribe: %v", err)
	}
	if s.queue != "order-service.events" {
		t.Fatalf("queue = %q, want order-service.events", s.queue)
	}
}

func TestSubscribeRawRequiresQueueName(t *testing.T) {
	if _, _, err := (&Subscribe{}).SubscribeRaw(context.Background()); err == nil {
		t.Fatal("expected error when queue name is empty")
	}
}

func TestNewSubscribeWithQueuePrefixAndQoS(t *testing.T) {
	s, err := NewSubscribe(nil, "eventbus.demo",
		WithQueueName("order.events"),
		WithQueuePrefix(" dev "),
		WithQoS(1),
	)
	if err != nil {
		t.Fatalf("NewSubscribe: %v", err)
	}
	if got := s.fullQueueName(); got != "dev_order.events" {
		t.Fatalf("fullQueueName = %q, want dev_order.events", got)
	}
	if s.qos != 1 {
		t.Fatalf("qos = %d, want 1", s.qos)
	}
}

func TestNewSubscribeRejectsNegativeQoS(t *testing.T) {
	if _, err := NewSubscribe(nil, "eventbus.demo",
		WithQueueName("order.events"),
		WithQoS(-1),
	); err == nil {
		t.Fatal("expected error for negative qos")
	}
}

func TestWithPublishTimeout(t *testing.T) {
	p := &Publish{}
	WithPublishTimeout(2 * time.Second)(p)
	if p.confirmTimeout != 2*time.Second {
		t.Fatalf("confirmTimeout = %v, want 2s", p.confirmTimeout)
	}
}
