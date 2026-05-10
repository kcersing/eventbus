package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
)

// Handler 事件处理器接口，所有订阅者必须实现此接口。
type Handler interface {
	Handle(ctx context.Context, event *Event) error
}

// HandlerFunc 将普通函数适配为 Handler 接口。
type HandlerFunc func(ctx context.Context, event *Event) error

func (f HandlerFunc) Handle(ctx context.Context, event *Event) error {
	return f(ctx, event)
}

// TypedHandler 泛型事件处理函数，Payload 自动反序列化为类型 T。
type TypedHandler[T any] func(ctx context.Context, payload T, event *Event) error

// WrapTyped 将 TypedHandler[T] 转换为标准 Handler。
// 转换顺序：直接断言 → map JSON 转换 → []byte JSON 转换。
func WrapTyped[T any](handler TypedHandler[T]) Handler {
	return HandlerFunc(func(ctx context.Context, event *Event) error {
		if v, ok := event.Payload.(T); ok {
			return handler(ctx, v, event)
		}
		if raw, ok := event.Payload.(map[string]any); ok {
			b, err := json.Marshal(raw)
			if err != nil {
				return fmt.Errorf("eventbus: marshal payload: %w", err)
			}
			var v T
			if err := json.Unmarshal(b, &v); err != nil {
				return fmt.Errorf("eventbus: unmarshal payload to %T: %w", new(T), err)
			}
			return handler(ctx, v, event)
		}
		if b, ok := event.Payload.([]byte); ok {
			var v T
			if err := json.Unmarshal(b, &v); err != nil {
				return fmt.Errorf("eventbus: unmarshal payload to %T: %w", new(T), err)
			}
			return handler(ctx, v, event)
		}
		return fmt.Errorf("eventbus: type mismatch: want %T, got %T", new(T), event.Payload)
	})
}
