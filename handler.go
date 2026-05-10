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

// EventHandler 事件处理函数类型（便捷别名）。
type EventHandler func(ctx context.Context, event *Event) error

// EventHandlerFunc 将普通函数适配为 Handler 接口。
// 用法：EventHandlerFunc(myFunc) 或直接传函数给需要 Handler 的地方。
type EventHandlerFunc func(ctx context.Context, event *Event) error

// Handle 实现 Handler 接口。
func (f EventHandlerFunc) Handle(ctx context.Context, event *Event) error {
	return f(ctx, event)
}

// TypedHandler 泛型事件处理函数，Payload 自动反序列化为类型 T。
// 配合 WrapTyped 使用可避免手动类型断言。
type TypedHandler[T any] func(ctx context.Context, payload T, event *Event) error

// WrapTyped 将泛型 TypedHandler[T] 转换为标准 Handler 接口。
// Payload 会通过直接断言或 JSON 反序列化转为 T 类型。
func WrapTyped[T any](handler TypedHandler[T]) Handler {

	return EventHandlerFunc(func(ctx context.Context, event *Event) error {
		// 1. 尝试直接断言
		if typedPayload, ok := event.Payload.(T); ok {
			return handler(ctx, typedPayload, event)
		}

		// 2. 如果是 map[string]interface{}, 尝试通过 json 转换
		if raw, ok := event.Payload.(map[string]interface{}); ok {
			jsonBytes, err := json.Marshal(raw)
			if err != nil {
				return fmt.Errorf("序列化 Payload 失败: %w", err)
			}
			var typedPayload T
			if err := json.Unmarshal(jsonBytes, &typedPayload); err != nil {
				return fmt.Errorf("反序列化 Payload 到 %T 失败: %w", new(T), err)
			}
			return handler(ctx, typedPayload, event)
		} else if raw, ok := event.Payload.([]byte); ok {
			var typedPayload T
			if err := json.Unmarshal(raw, &typedPayload); err != nil {
				return fmt.Errorf("反序列化 Payload 到 %T 失败: %w", new(T), err)
			}
			return handler(ctx, typedPayload, event)
		}
		// 3. 如果以上都不行，返回类型不匹配错误
		return fmt.Errorf("类型不匹配: 期望 %T 或 map[string]interface{}, 实际 %T", new(T), event.Payload)
	})
}
