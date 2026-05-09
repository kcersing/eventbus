package eventbus

// Package eventbus 是一个轻量级事件总线库，支持进程内发布/订阅和 RabbitMQ 分布式事件桥接。
//
// 核心组件：
//   - EventBus：内存事件总线，支持 topic 订阅、中间件链、通配符匹配（*）
//   - EventPublisher：统一发布入口，三种作用域（Local / Distributed / MQOnly）
//   - ConsumerPool：高吞吐消费者池，支持并发、超时、指数退避重试、死信处理
//   - ConsumerRegistry：集中注册消费者，统一启停
//   - AMQPListener：RabbitMQ → 内存总线的单向桥接（MQ 消息转发到本地）
//   - EventManager：统一管理上述所有组件的生命周期
//
// 基本用法：
//
//	bus := eventbus.NewEventBus()
//	bus.SubscribeAsync(ctx, "order.created", handler, 4)
//	bus.PublishByTopic(ctx, "order.created", payload)

import "context"

// Subscription 代表一个活跃订阅，调用 Unsubscribe 可安全取消并释放资源。
// 同一 Subscription 多次调用 Unsubscribe 是安全的（幂等）。
type Subscription interface {
	Unsubscribe()
}

// Subscribe 定义了订阅相关操作。
type Subscribe interface {
	// Subscribe 同步订阅，返回原始事件通道（需自行管理 goroutine）。
	Subscribe(ctx context.Context, topic string) EventChan
	// SubscribeAsync 异步订阅，自动启动 concurrency 个 goroutine 消费事件。
	SubscribeAsync(ctx context.Context, topic string, handler Handler, concurrency int) Subscription
	// SubscribeWithPool 使用 ConsumerPool 订阅，支持设置重试、超时、死信等。
	SubscribeWithPool(ctx context.Context, topic string, handler Handler, workerNum int32, opts ...func(*PoolOptions)) Subscription
	// Unsubscribe 取消订阅。
	// Deprecated: 请使用 Subscription.Unsubscribe() 代替。
	Unsubscribe(topic string, ch EventChan)
}

// Publish 定义了事件发布操作。
type Publish interface {
	Publish(ctx context.Context, event *Event)
}

// Bus 定义了总线级别的操作（中间件注册和生命周期管理）。
type Bus interface {
	// Use 注册中间件，按注册顺序执行。
	Use(mw ...Middleware)
	// Close 优雅关闭总线，先取消所有活跃订阅，再清理残留通道。
	Close() error
}
