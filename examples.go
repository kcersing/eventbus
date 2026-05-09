package eventbus

import (
	"context"
	"time"

	"github.com/kcersing/amqpclt"
)

// --- 1. 定义事件结构体 ---

// UserRegisteredEvent 用户注册事件的载荷
type UserRegisteredEvent struct {
	UserID   string
	Username string
}

// OrderCreatedEvent 订单创建事件 (用于注册表示例)
type OrderCreatedEvent struct {
	OrderID string
	Amount  float64
}

// InternalTaskEvent 服务内部任务的载荷
type InternalTaskEvent struct {
	TaskID      string
	Description string
}

// NotificationEvent 定义一个用于发送到MQ的通知事件
type NotificationEvent struct {
	Recipient string
	Message   string
}

// --- 2. 定义事件处理器 ---

// ... (previous handlers) ...
func handleAdvancedUserRegistered(ctx context.Context, payload UserRegisteredEvent, event Event) error {
	logInfo("[Handler-Advanced] 收到用户注册: UserID=%s, EventID=%s", payload.UserID, event.Id)
	return nil
}
func handleAdvancedInternalTask(ctx context.Context, payload InternalTaskEvent, event Event) error {
	logInfo("[Handler-Advanced] 处理内部任务: TaskID=%s, Desc: %s", payload.TaskID, event.Id)
	return nil
}
func handleRegistryUserRegistered(ctx context.Context, payload UserRegisteredEvent, event Event) error {
	logInfo("[Handler-Registry] 用户注册: UserID=%s", payload.UserID)
	return nil
}
func handleRegistryOrderCreated(ctx context.Context, payload OrderCreatedEvent, event Event) error {
	logInfo("[Handler-Registry] 新订单: OrderID=%s, Amount=%.2f", payload.OrderID, payload.Amount)
	return nil
}
func handleRegistryOrderAnalytics(ctx context.Context, payload OrderCreatedEvent, event Event) error {
	logInfo("[Handler-Registry-Analytics] 记录订单数据: OrderID=%s", payload.OrderID)
	return nil
}

// --- 3. 运行高级示例 (手动订阅) ---
func RunAdvancedExample() { /* ... */ }

// --- 4. 运行消费者注册表示例 ---
func RunRegistryExample() { /* ... */ }

// --- 5. 运行只发往MQ的示例 ---

func RunMQOnlyExample() {
	logInfo("\n\n--- 开始运行只发往MQ的示例 ---")
	bus := NewEventBus()
	defer bus.Close()

	// 使用模拟的AMQP发布者初始化EventPublisher
	mockPublisher, _ := amqpclt.NewPublisher(
		nil,
		"notification_exchange",
	)
	publisher := NewEventPublisher(bus, mockPublisher)

	//	bus.Subscribe(ctx, "order.*")     // 匹配 order.created、order.paid、order.cancelled                     ││                                        │
	//	bus.Subscribe(ctx, "*.created")   // 匹配 user.created、order.created                                    ││                                        │
	//	bus.Subscribe(ctx, "order.created") // 精确匹配，不受影响
	// 关键：在本地订阅 "notification.sent" 主题，以验证它不会收到消息
	localSub := bus.SubscribeAsync(context.Background(),
		"notification.sent",
		EventHandlerFunc(func(ctx context.Context, event *Event) error {
			// 如果这个处理器被调用，说明测试失败了
			logError("[MQOnly-FAIL] 本地订阅者不应该收到 ScopeMQOnly 的事件!")
			return nil
		}),
		1,
	)
	defer localSub.Unsubscribe()

	logInfo("\n--- [MQOnly] 发布一个 ScopeMQOnly 事件 ---")
	err := publisher.Publish(
		context.Background(),
		"notification.sent",
		NotificationEvent{Recipient: "test@example.com", Message: "Hello, World!"},
		WithScope(ScopeMQOnly), // 明确指定只发送到MQ
	)
	if err != nil {
		logError("[MQOnly] 发布事件失败: %v", err)
	}

	logInfo("\n--- [MQOnly] 发布一个 ScopeDistributed 事件作为对比 ---")
	// 这个事件应该同时触发 MockAMQP 和本地订阅者
	_ = publisher.Publish(
		context.Background(),
		"notification.sent",
		NotificationEvent{Recipient: "another@example.com", Message: "Distributed Message"},
		WithScope(ScopeDistributed),
	)

	logInfo("\n--- [MQOnly] 等待1秒观察结果 ---")
	time.Sleep(1 * time.Second)

	logInfo("--- 只发往MQ的示例运行结束 ---")
	logInfo("预期结果: 只有'Distributed Message'事件会触发本地订阅者的失败日志。")
}

// Middleware 例子
// loggingPlugin 日志记录
func loggingPlugin() Middleware {
	return func(next Handler) Handler {
		return EventHandlerFunc(func(ctx context.Context, event *Event) error {
			start := time.Now()
			err := next.Handle(ctx, event)
			logInfo("[日志插件] Done: Topic=%s cost=%v", event.Topic, time.Since(start))
			return err
		})
	}
}

// filterPlugin 消息过滤
func filterPlugin(filterTopic string) Middleware {
	return func(next Handler) Handler {
		return EventHandlerFunc(func(ctx context.Context, event *Event) error {
			// 只有当主题不是我们想过滤的主题时才继续
			if event.Topic == filterTopic {
				logWarn("[过滤插件] 过滤掉主题为 '%s' 的事件\n", filterTopic)
				return nil // 中止执行链，事件不会被分发
			}
			return next.Handle(ctx, event)
		})
	}
}

// transformPlugin 消息转换
func transformPlugin() Middleware {
	return func(next Handler) Handler {
		return EventHandlerFunc(func(ctx context.Context, event *Event) error {
			if event.Topic == "order" {
				// 假设负载是字符串，我们给它添加一个前缀
				if originalPayload, ok := event.Payload.(string); ok {
					event.Payload = "已转换: " + originalPayload
					logWarn("[转换插件] 转换订单事件 Payload\n")
				}
			}
			return next.Handle(ctx, event) // 传递修改后的事件

		})
	}
}

// recoverPlugin 捕获 handler 中的 panic，防止单个 panic 崩溃整个进程
func recoverPlugin() Middleware {
	return func(next Handler) Handler {
		return EventHandlerFunc(func(ctx context.Context, event *Event) error {
			defer func() {
				if r := recover(); r != nil {
					logError("[RecoverPlugin] panic recovered, topic=%s, err=%v", event.Topic, r)
				}
			}()
			return next.Handle(ctx, event)
		})
	}
}
