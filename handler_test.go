package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// - TestHandlerFunc — HandlerFunc 适配器基本功能
// - TestWrapTyped_DirectAssert — Payload 直接类型断言
// - TestWrapTyped_MapConversion — map[string]any → JSON 转换
// - TestWrapTyped_BytesConversion — []byte → JSON 反序列化
// - TestWrapTyped_TypeMismatch / TestWrapTyped_HandlerError — 类型不匹配和错误传播
func TestHandlerFunc(t *testing.T) {
	called := false
	h := HandlerFunc(func(ctx context.Context, event *Event) error {
		called = true
		return nil
	})
	err := h.Handle(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("HandlerFunc was not called")
	}
}

func TestWrapTyped_DirectAssert(t *testing.T) {
	type Order struct {
		ID int `json:"id"`
	}
	var received Order
	h := WrapTyped(func(ctx context.Context, o Order, e *Event) error {
		received = o
		return nil
	})

	event := NewEvent("test", Order{ID: 99})
	err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if received.ID != 99 {
		t.Errorf("received.ID = %d, want 99", received.ID)
	}
}

func TestWrapTyped_MapConversion(t *testing.T) {
	type Order struct {
		ID int `json:"id"`
	}
	var received Order
	h := WrapTyped(func(ctx context.Context, o Order, e *Event) error {
		received = o
		return nil
	})

	event := NewEvent("test", map[string]any{"id": float64(42)})
	err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if received.ID != 42 {
		t.Errorf("received.ID = %d, want 42", received.ID)
	}
}

func TestWrapTyped_BytesConversion(t *testing.T) {
	type Order struct {
		ID int `json:"id"`
	}
	var received Order
	h := WrapTyped(func(ctx context.Context, o Order, e *Event) error {
		received = o
		return nil
	})

	b, _ := json.Marshal(Order{ID: 7})
	event := NewEvent("test", b)
	err := h.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if received.ID != 7 {
		t.Errorf("received.ID = %d, want 7", received.ID)
	}
}

func TestWrapTyped_TypeMismatch(t *testing.T) {
	type Order struct{ ID int }
	h := WrapTyped(func(ctx context.Context, o Order, e *Event) error { return nil })

	event := NewEvent("test", "not an order")
	err := h.Handle(context.Background(), event)
	if err == nil {
		t.Error("expected type mismatch error, got nil")
	}
}

func TestWrapTyped_HandlerError(t *testing.T) {
	wantErr := errors.New("handler failed")
	h := WrapTyped(func(ctx context.Context, s string, e *Event) error {
		return wantErr
	})

	event := NewEvent("test", "hello")
	err := h.Handle(context.Background(), event)
	if err != wantErr {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
}
