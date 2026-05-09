package eventbus

import "context"

// Subscription 代表一个订阅，并提供了取消订阅的方法
type Subscription interface {
	Unsubscribe()
}

type Subscribe interface {
	Subscribe(topic string) EventChan
	SubscribeAsync(topic string, handler Handler, concurrency int) Subscription
	// SubscribeWithPool 使用消费者池订阅，可自定义池的配置
	SubscribeWithPool(topic string, handler Handler, workerNum int32, opts ...func(*PoolOptions)) Subscription
	Unsubscribe(topic string, ch EventChan) // Deprecated: 将逐步由 Subscription.Unsubscribe() 替代
}

type Publish interface {
	Publish(ctx context.Context, event *Event)
}

type Bus interface {
	Use(mw ...Middleware)
	Close() error
}
