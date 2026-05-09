package eventbus

import (
	"context"
	"time"

	"github.com/kcersing/amqpclt"

	"github.com/cloudwego/kitex/pkg/klog"
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
	klog.Infof("[Handler-Advanced] 收到用户注册: UserID=%s, EventID=%s", payload.UserID, event.Id)
	return nil
}
func handleAdvancedInternalTask(ctx context.Context, payload InternalTaskEvent, event Event) error {
	klog.Infof("[Handler-Advanced] 处理内部任务: TaskID=%s, Desc: %s", payload.TaskID, event.Id)
	return nil
}
func handleRegistryUserRegistered(ctx context.Context, payload UserRegisteredEvent, event Event) error {
	klog.Infof("[Handler-Registry] 用户注册: UserID=%s", payload.UserID)
	return nil
}
func handleRegistryOrderCreated(ctx context.Context, payload OrderCreatedEvent, event Event) error {
	klog.Infof("[Handler-Registry] 新订单: OrderID=%s, Amount=%.2f", payload.OrderID, payload.Amount)
	return nil
}
func handleRegistryOrderAnalytics(ctx context.Context, payload OrderCreatedEvent, event Event) error {
	klog.Infof("[Handler-Registry-Analytics] 记录订单数据: OrderID=%s", payload.OrderID)
	return nil
}

// --- 3. 运行高级示例 (手动订阅) ---
func RunAdvancedExample() { /* ... */ }

// --- 4. 运行消费者注册表示例 ---
func RunRegistryExample() { /* ... */ }

// --- 5. 运行只发往MQ的示例 ---

func RunMQOnlyExample() {
	klog.Info("\n\n--- 开始运行只发往MQ的示例 ---")
	bus := NewEventBus()
	defer bus.Close()
	// 创建模拟的AMQP发布者

	// 使用模拟的AMQP发布者初始化EventPublisher
	mockPublisher, _ := amqpclt.NewPublisher(
		nil,
		"notification_exchange",
	)
	publisher := NewEventPublisher(bus, mockPublisher)

	// 关键：在本地订阅 "notification.sent" 主题，以验证它不会收到消息
	localSub := bus.SubscribeAsync(
		"notification.sent",
		EventHandlerFunc(func(ctx context.Context, event *Event) error {
			// 如果这个处理器被调用，说明测试失败了
			klog.Errorf("[MQOnly-FAIL] 本地订阅者不应该收到 ScopeMQOnly 的事件!")
			return nil
		}),
		1,
	)
	defer localSub.Unsubscribe()

	klog.Info("\n--- [MQOnly] 发布一个 ScopeMQOnly 事件 ---")
	err := publisher.Publish(
		context.Background(),
		"notification.sent",
		NotificationEvent{Recipient: "test@example.com", Message: "Hello, World!"},
		WithScope(ScopeMQOnly), // 明确指定只发送到MQ
	)
	if err != nil {
		klog.Errorf("[MQOnly] 发布事件失败: %v", err)
	}

	klog.Info("\n--- [MQOnly] 发布一个 ScopeDistributed 事件作为对比 ---")
	// 这个事件应该同时触发 MockAMQP 和本地订阅者
	_ = publisher.Publish(
		context.Background(),
		"notification.sent",
		NotificationEvent{Recipient: "another@example.com", Message: "Distributed Message"},
		WithScope(ScopeDistributed),
	)

	klog.Info("\n--- [MQOnly] 等待1秒观察结果 ---")
	time.Sleep(1 * time.Second)

	klog.Info("--- 只发往MQ的示例运行结束 ---")
	klog.Info("预期结果: 只有'Distributed Message'事件会触发本地订阅者的失败日志。")
}
