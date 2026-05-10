package manager

import (
	"context"
	"fmt"
	"sync"

	"eventbus"

	"eventbus/ext/pool"
)

// ConsumerConfig 绑定一个 Handler 到指定 topic 和池参数。
type ConsumerConfig struct {
	Topic     string
	Handler   eventbus.Handler
	WorkerNum int
	PoolOpts  []pool.Option
}

// Registry 集中注册消费者，统一启停。
//
// 用法：
//
//	r := manager.NewRegistry()
//	r.Register("order.created", handler, 4, pool.WithMaxRetries(3))
//	r.StartAll(ctx, bus)
//	defer r.Shutdown()
type Registry struct {
	mu      sync.Mutex
	configs []ConsumerConfig
	subs    []eventbus.Subscription
}

// NewRegistry 创建消费者注册表。
func NewRegistry() *Registry {
	return &Registry{}
}

// Register 注册一个消费者配置，StartAll 前调用。
func (r *Registry) Register(topic string, handler eventbus.Handler, workerNum int, opts ...pool.Option) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.configs = append(r.configs, ConsumerConfig{
		Topic:     topic,
		Handler:   handler,
		WorkerNum: workerNum,
		PoolOpts:  opts,
	})
}

// StartAll 启动所有已注册消费者，不可重复调用。
func (r *Registry) StartAll(ctx context.Context, bus *eventbus.EventBus) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.subs) > 0 {
		return fmt.Errorf("manager: registry already started")
	}
	for _, cfg := range r.configs {
		sub := pool.SubscribeWithPool(ctx, bus, cfg.Topic, cfg.Handler, cfg.WorkerNum, cfg.PoolOpts...)
		r.subs = append(r.subs, sub)
	}
	return nil
}

// Shutdown 取消所有由该注册表启动的订阅。
func (r *Registry) Shutdown() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, sub := range r.subs {
		sub.Unsubscribe()
	}
	r.subs = nil
	return nil
}
