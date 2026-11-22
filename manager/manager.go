// Package manager 提供统一的事件组件生命周期管理，作为 eventbus 扩展包的可选组合层。
//
// 如果你只使用核心 eventbus 包，不需要引入此包。
// 此包适用于同时使用 amqp + pool 扩展，需要统一启停的场景。
package manager

import (
	"context"
	"errors"

	"github.com/kcersing/eventbus"

	extamqp "github.com/kcersing/eventbus/ext/amqp"
)

// Lifecycle 是 Manager 依赖的最小组件生命周期能力。
// ext/amqp.Listener 实现了该接口；测试也可以注入自定义实现。
type Lifecycle interface {
	Start(ctx context.Context) error
	Stop()
}

// Manager 统一管理所有事件组件的生命周期。
type Manager struct {
	Bus       *eventbus.EventBus
	Listener  Lifecycle          // 可选，MQ → 内存总线
	Publisher *extamqp.Publisher // 可选，统一发布入口
	Registry  *Registry          // 可选，消费者注册表
}

// New 创建 Manager，所有字段均为可选（nil 表示不启用）。
func New(
	bus *eventbus.EventBus,
	listener Lifecycle,
	publisher *extamqp.Publisher,
	registry *Registry,
) *Manager {
	return &Manager{
		Bus:       bus,
		Listener:  listener,
		Publisher: publisher,
		Registry:  registry,
	}
}

// Start 启动 AMQP 监听器和所有已注册消费者。
func (m *Manager) Start(ctx context.Context) error {
	if m.Listener != nil {
		if err := m.Listener.Start(ctx); err != nil {
			return err
		}
	}
	if m.Registry != nil {
		if err := m.Registry.StartAll(ctx, m.Bus); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown 按序优雅关闭：Listener → Registry → Publisher → Bus。
func (m *Manager) Shutdown(ctx context.Context) error {
	var errs []error

	if m.Listener != nil {
		m.Listener.Stop()
	}
	if m.Registry != nil {
		if err := m.Registry.Shutdown(); err != nil {
			errs = append(errs, err)
		}
	}
	if m.Publisher != nil {
		if err := m.Publisher.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if m.Bus != nil {
		if err := m.Bus.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
