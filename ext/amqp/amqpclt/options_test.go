package amqpclt

import (
	"context"
	"testing"

	"github.com/bytedance/sonic"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDLXNames(t *testing.T) {
	dlx, dlq := DLXNames("orders")
	if dlx != "orders.dlx" || dlq != "orders.dlq" {
		t.Fatalf("DLXNames = (%q, %q), want (orders.dlx, orders.dlq)", dlx, dlq)
	}
}

func TestFullQueueName(t *testing.T) {
	tests := []struct {
		queue  string
		prefix string
		want   string
	}{
		{"orders", "", "orders"},
		{"orders", "dev", "dev_orders"},
		{" orders ", " dev ", "dev_orders"},
		{"orders", "   ", "orders"},
	}
	for _, tc := range tests {
		s := &Subscribe{queue: tc.queue, queuePrefix: tc.prefix}
		if got := s.fullQueueName(); got != tc.want {
			t.Fatalf("fullQueueName(queue=%q, prefix=%q) = %q, want %q",
				tc.queue, tc.prefix, got, tc.want)
		}
	}
}

func TestNewSubscribeDefaults(t *testing.T) {
	s, err := NewSubscribe(nil, "eventbus.demo", WithQueueName("order.events"))
	if err != nil {
		t.Fatalf("NewSubscribe: %v", err)
	}
	if s.qos != 3 {
		t.Fatalf("default qos = %d, want 3", s.qos)
	}
	if s.dlx != "" {
		t.Fatalf("default dlx = %q, want empty", s.dlx)
	}
}

func TestSubscribeOptions(t *testing.T) {
	s := &Subscribe{}
	WithDeadLetterExchange("eventbus.dlx")(s)
	WithQoS(0)(s)

	if s.dlx != "eventbus.dlx" {
		t.Fatalf("dlx = %q, want eventbus.dlx", s.dlx)
	}
	if s.qos != 0 {
		t.Fatalf("qos = %d, want 0", s.qos)
	}
}

func TestNewSubscribeAllowsZeroQoS(t *testing.T) {
	if _, err := NewSubscribe(nil, "eventbus.demo",
		WithQueueName("order.events"),
		WithQoS(0),
	); err != nil {
		t.Fatalf("NewSubscribe with qos 0: %v", err)
	}
}

type decodedOrder struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// 已注册的 payloadType 应在解码时恢复为具体类型。
func TestDecodeMessageRestoresRegisteredPayloadType(t *testing.T) {
	const payloadType = "amqpclt.test.decodedOrder"
	RegisterPayloadType(payloadType, func() interface{} { return &decodedOrder{} })

	body, err := sonic.Marshal(Message{
		Event:       "order.created",
		PayloadType: payloadType,
		Payload:     map[string]any{"id": 7, "name": "demo"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	msg, err := decodeMessage(context.Background(), amqp.Delivery{Body: body})
	if err != nil {
		t.Fatalf("decodeMessage: %v", err)
	}

	order, ok := msg.Payload.(*decodedOrder)
	if !ok {
		t.Fatalf("payload type = %T, want *decodedOrder", msg.Payload)
	}
	if order.ID != 7 || order.Name != "demo" {
		t.Fatalf("order = %+v, want {ID:7 Name:demo}", *order)
	}
}

// 未注册的 payloadType 保持 map 形式。
func TestDecodeMessageUnknownPayloadTypeKeepsMap(t *testing.T) {
	body, err := sonic.Marshal(Message{
		Event:       "order.created",
		PayloadType: "amqpclt.test.unknown",
		Payload:     map[string]any{"id": 1},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	msg, err := decodeMessage(context.Background(), amqp.Delivery{Body: body})
	if err != nil {
		t.Fatalf("decodeMessage: %v", err)
	}

	m, ok := msg.Payload.(map[string]interface{})
	if !ok {
		t.Fatalf("payload type = %T, want map", msg.Payload)
	}
	if got, ok := m["id"].(float64); !ok || got != 1 {
		t.Fatalf("payload id = %v (%T), want 1", m["id"], m["id"])
	}
}

// payloadType 已注册但 payload 不是 map 时，保留原值。
func TestDecodeMessagePayloadTypeWithNonMapPayload(t *testing.T) {
	const payloadType = "amqpclt.test.nonMap"
	RegisterPayloadType(payloadType, func() interface{} { return &decodedOrder{} })

	body, err := sonic.Marshal(Message{
		Event:       "order.created",
		PayloadType: payloadType,
		Payload:     "raw",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	msg, err := decodeMessage(context.Background(), amqp.Delivery{Body: body})
	if err != nil {
		t.Fatalf("decodeMessage: %v", err)
	}
	if msg.Payload != "raw" {
		t.Fatalf("payload = %v (%T), want raw", msg.Payload, msg.Payload)
	}
}
