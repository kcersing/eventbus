package eventbus

import (
	"context"
	"fmt"
	"sync"
)

// ============ 消费者注册表 ============

// ConsumerConfig 消费者配置，将 Handler + Topic + 池参数绑定为一个消费单元。
type ConsumerConfig struct {
	Topic       string               // 事件主题
	HandlerName string               // 处理器名字
	WorkerNum   int32                // 工作线程数
	PoolOpts    []func(*PoolOptions) // 消费者池的配置选项
}

// ConsumerRegistry 集中管理消费者的注册和生命周期。
// 先 RegisterHandler 注册处理函数，再 RegisterConsumer 绑定 topic → 最后 StartAll 一键启动。
type ConsumerRegistry struct {
	mu            sync.RWMutex
	handlers      map[string]Handler // handlerName → Handler
	configs       []*ConsumerConfig  // 存储所有消费者配置
	subscriptions []Subscription     // 存储所有激活的订阅
}

// NewConsumerRegistry 创建消费者注册表
func NewConsumerRegistry() *ConsumerRegistry {
	return &ConsumerRegistry{
		handlers:      make(map[string]Handler),
		configs:       []*ConsumerConfig{},
		subscriptions: []Subscription{},
	}
}

// RegisterHandler 注册处理器
func (cr *ConsumerRegistry) RegisterHandler(name string, handler Handler) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if _, exists := cr.handlers[name]; exists {
		return fmt.Errorf("处理器 %s 已存在", name)
	}

	cr.handlers[name] = handler
	logInfo("[注册表] 处理器已注册: %s", name)
	return nil
}

// RegisterConsumer 注册消费者，并可选择性地提供池配置
func (cr *ConsumerRegistry) RegisterConsumer(topic, handlerName string, workerNum int32, opts ...func(*PoolOptions)) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if _, exists := cr.handlers[handlerName]; !exists {
		return fmt.Errorf("处理器 %s 不存在", handlerName)
	}

	config := &ConsumerConfig{
		Topic:       topic,
		HandlerName: handlerName,
		WorkerNum:   workerNum,
		PoolOpts:    opts,
	}

	cr.configs = append(cr.configs, config)
	logInfo("[注册表] 消费者已配置: topic=%s, handler=%s, workers=%d",
		topic, handlerName, workerNum)
	return nil
}

// StartAll 启动所有已注册的消费者
func (cr *ConsumerRegistry) StartAll(eb *EventBus) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if len(cr.subscriptions) > 0 {
		return fmt.Errorf("注册表已经启动")
	}

	for _, config := range cr.configs {
		handler, ok := cr.handlers[config.HandlerName]
		if !ok {
			// 这在正常情况下不应该发生，因为RegisterConsumer已经检查过了
			logError("[注册表] 严重错误: 启动时未找到处理器 %s", config.HandlerName)
			continue
		}

		// 使用新的接口和选项来订阅
		subscription := eb.SubscribeWithPool(context.Background(), config.Topic, handler, config.WorkerNum, config.PoolOpts...)
		cr.subscriptions = append(cr.subscriptions, subscription)

		logInfo("[注册表] 消费者已启动: topic=%s, handler=%s, workers=%d",
			config.Topic, config.HandlerName, config.WorkerNum)
	}
	return nil
}

// Shutdown 关闭所有由该注册表启动的消费者
func (cr *ConsumerRegistry) Shutdown(ctx context.Context) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	logInfo("[注册表] 正在关闭所有消费者...")
	for _, sub := range cr.subscriptions {
		sub.Unsubscribe()
	}
	// 清空订阅列表，允许再次启动
	cr.subscriptions = []Subscription{}
	logInfo("[注册表] 所有消费者已关闭")
	return nil
}

// GetAllConfigs 获取所有消费者配置
func (cr *ConsumerRegistry) GetAllConfigs() []*ConsumerConfig {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	// 返回一个副本以保证线程安全
	result := make([]*ConsumerConfig, len(cr.configs))
	copy(result, cr.configs)
	return result
}
