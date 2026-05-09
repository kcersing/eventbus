package eventbus

import (
	"context"

)

// EventManager 统一管理所有事件组件的生命周期（EventBus、AMQPListener、ConsumerRegistry、EventPublisher）。
// 调用 Start 一键启动所有组件，Shutdown 按序优雅关闭。
type EventManager struct {
	Bus       *EventBus
	Bridge    *AMQPListener
	Registry  *ConsumerRegistry
	Publisher *EventPublisher
}

// NewEventManager 创建 EventManager 实例。
func NewEventManager(
	bus *EventBus,
	bridge *AMQPListener,
	registry *ConsumerRegistry,
	publisher *EventPublisher,
) *EventManager {
	return &EventManager{
		Bus:       bus,
		Bridge:    bridge,
		Registry:  registry,
		Publisher: publisher,
	}
}

// Start 启动 AMQP 监听器和消费者注册表中的所有消费者。
func (em *EventManager) Start(ctx context.Context) error {
	logInfo("[EventManager] Starting all components...")
	if err := em.Bridge.StartListener(ctx); err != nil {
		return err
	}
	if err := em.Registry.StartAll(em.Bus); err != nil {
		return err
	}
	logInfo("[EventManager] All components started successfully.")
	return nil
}

// Shutdown 按序优雅关闭：Bridge.Stop → Registry.Shutdown → Bus.Close → Publisher.Close。
func (em *EventManager) Shutdown(ctx context.Context) error {
	if err := em.Bridge.Stop(); err != nil {
		return err
	}
	if err := em.Registry.Shutdown(ctx); err != nil {
		return err
	}
	if err := em.Bus.Close(); err != nil {
		return err
	}
	if err := em.Publisher.Close(); err != nil {
		return err
	}
	return nil
}
