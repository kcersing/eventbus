package eventbus

import (
	"context"

	"github.com/cloudwego/kitex/pkg/klog"
)

// EventManager 统一管理事件总线、桥接器和消费者注册表
type EventManager struct {
	Bus       *EventBus
	Bridge    *AMQPListener
	Registry  *ConsumerRegistry
	Publisher *EventPublisher
}

// NewEventManager 是 EventManager 的构造函数
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

// Start 启动所有组件
func (em *EventManager) Start(ctx context.Context) error {
	klog.Info("[EventManager] Starting all components...")
	if err := em.Bridge.StartListener(ctx); err != nil {
		return err
	}
	if err := em.Registry.StartAll(em.Bus); err != nil {
		return err
	}
	klog.Info("[EventManager] All components started successfully.")
	return nil
}

// Shutdown 优雅地关闭整个事件系统
func (em *EventManager) Shutdown(ctx context.Context) error {
	klog.Info("[EventManager] Shutting down EventManager...")

	if err := em.Bridge.Stop(); err != nil {
		klog.Errorf("[EventManager] Failed to stop AMQP listener: %v", err)
	}

	if err := em.Registry.Shutdown(ctx); err != nil {
		klog.Errorf("[EventManager] Failed to shutdown consumer registry: %v", err)
	}

	klog.Info("[EventManager] EventManager shut down complete.")
	return nil
}
