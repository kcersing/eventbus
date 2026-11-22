// Package main 演示核心事件总线的常用能力：
// 精确订阅、通配符订阅、中间件、TypedHandler。
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/kcersing/eventbus"
)

// OrderCreated 是示例业务负载，用于演示 WrapTyped。
type OrderCreated struct {
	ID     int     `json:"id"`
	Amount float64 `json:"amount"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	bus := eventbus.New()
	defer bus.Close()

	// 中间件：按注册顺序执行（洋葱模型）。
	bus.Use(func(next eventbus.Handler) eventbus.Handler {
		return eventbus.HandlerFunc(func(ctx context.Context, e *eventbus.Event) error {
			fmt.Printf("[middleware] topic=%s id=%s\n", e.Topic, e.Id)
			return next.Handle(ctx, e)
		})
	})

	// 精确订阅 + 泛型 Handler：Payload 会被转换成 OrderCreated。
	orderSub := bus.SubscribeAsync(ctx, "order.created", eventbus.WrapTyped(
		func(ctx context.Context, order OrderCreated, e *eventbus.Event) error {
			fmt.Printf("[order.created] id=%d amount=%.2f event=%s\n", order.ID, order.Amount, e.Id)
			return nil
		}), 2)
	defer orderSub.Unsubscribe()

	// 通配符订阅：order.* 会匹配 order.created、order.paid 等。
	wildcardSub := bus.SubscribeAsync(ctx, "order.*", eventbus.HandlerFunc(
		func(ctx context.Context, e *eventbus.Event) error {
			fmt.Printf("[order.*] topic=%s payload=%v\n", e.Topic, e.Payload)
			return nil
		}), 1)
	defer wildcardSub.Unsubscribe()

	// 发布事件。
	bus.PublishTopic(ctx, "order.created", OrderCreated{ID: 1001, Amount: 99.5})
	bus.PublishTopic(ctx, "order.paid", map[string]any{"id": 1001})
	bus.PublishTopic(ctx, "user.created", "不会被 order.* 匹配")

	// 示例等待异步 Handler 执行完；生产环境应使用业务同步点或 WaitGroup。
	time.Sleep(300 * time.Millisecond)
	fmt.Println("done")
}
