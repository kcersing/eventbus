package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
)

type Handler interface {
	Handle(ctx context.Context, event *Event) error
}

// EventHandler 是订阅者处理
type EventHandler func(ctx context.Context, event *Event) error

// EventHandlerFunc 定义函数，它适配了 Handler 接口
type EventHandlerFunc func(ctx context.Context, event *Event) error

// Handle 实现 Handler 接口
func (f EventHandlerFunc) Handle(ctx context.Context, event *Event) error {
	return f(ctx, event)
}

// TypedHandler 是一个泛型函数：支持类型安全的处理
type TypedHandler[T any] func(ctx context.Context, payload T, event Event) error

// WrapTyped 把泛型处理函数转换为标准 Handler 接口
func WrapTyped[T any](handler TypedHandler[T]) Handler {

	return EventHandlerFunc(func(ctx context.Context, event *Event) error {
		// 1. 尝试直接断言
		if typedPayload, ok := event.Payload.(T); ok {
			return handler(ctx, typedPayload, *event)
		}

		// 2. 如果是 map[string]interface{}, 尝试通过 json 转换
		if mapPayload, ok := event.Payload.(map[string]interface{}); ok {
			jsonBytes, err := json.Marshal(mapPayload)
			if err != nil {
				return fmt.Errorf("failed to marshal payload map: %w", err)
			}

			var typedPayload T
			if err := json.Unmarshal(jsonBytes, &typedPayload); err != nil {
				return fmt.Errorf("failed to unmarshal payload into %T: %w", new(T), err)
			}
			return handler(ctx, typedPayload, *event)
		}

		// 3. 如果以上都不行，返回类型不匹配错误
		return fmt.Errorf("type mismatch: expected %T or map[string]interface{}, got %T", new(T), event.Payload)
	})
}
