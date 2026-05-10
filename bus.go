package eventbus

import (
	"context"
	"path"
	"strings"
	"sync"
	"sync/atomic"
)

// EventChan 事件通道，Subscribe() 返回此类型供用户自行消费。
type (
	EventChan chan *Event
)

var _ Bus = (*EventBus)(nil)
var _ Publish = (*EventBus)(nil)
var _ Subscribe = (*EventBus)(nil)

// wildcardEntry 通配符订阅条目，将同一 pattern 的订阅者分组管理。
type wildcardEntry struct {
	pattern  string      // 通配符模式，如 "order.*"
	channels []EventChan // 该模式下的所有订阅通道
}

// EventBus 内存事件总线，支持 topic 订阅、中间件链和通配符匹配。
// 线程安全，可并发读写。
type EventBus struct {
	mu           sync.RWMutex
	subscribers  map[string][]EventChan // 精确匹配订阅
	wildcardSubs []wildcardEntry        // 通配符订阅（* 匹配单层）
	middlewares  []Middleware           // 中间件
	// Fix #3: 用 atomic.Value 替代普通字段，消除 Publish 与 Use 之间的并发读写竞争。
	// Publish 无锁读、Use 持写锁后原子替换，两者不再竞争同一内存地址。
	chain      atomic.Value // stores Handler
	activeSubs []Subscription // 活跃订阅列表（用于 Close 时统一取消）
	subsMu     sync.Mutex
	metrics    Metrics // 可观测性接口
}

// NewEventBus 创建事件总线
func NewEventBus() *EventBus {
	eb := &EventBus{
		subscribers: make(map[string][]EventChan),
		middlewares: []Middleware{},
		activeSubs:  []Subscription{},
		metrics:     noopMetrics{},
	}
	eb.rebuildChain() // 初始化链
	return eb
}

// dispatch 将事件分发到所有匹配的订阅者通道。
// 短路径：单订阅者+无通配符 → 零分配直接发送。
// Fix #7: 先遍历通配符统计真实 channel 数量，再精确分配切片容量，避免 wildcardLen*2 的错误估算。
func (eb *EventBus) dispatch(ctx context.Context, event *Event) error {
	eb.mu.RLock()
	exact := eb.subscribers[event.Topic]
	exactLen := len(exact)
	wildcardLen := len(eb.wildcardSubs)

	if exactLen == 0 && wildcardLen == 0 {
		eb.mu.RUnlock()
		return nil
	}

	// 短路径：只有单个精确订阅者，无通配符，零分配
	if exactLen == 1 && wildcardLen == 0 {
		subscriber := exact[0]
		eb.mu.RUnlock()
		eb.safeSend(subscriber, event)
		return nil
	}

	// Fix #7: 先统计通配符命中的真实 channel 总数，再一次性精确分配。
	// 原来用 wildcardLen*2 作为容量上限，wildcardLen 是 entry 数而非 channel 数，完全不准确。
	matchedWildcard := make([][]EventChan, 0, wildcardLen)
	extraLen := 0
	for i := range eb.wildcardSubs {
		if ok, _ := path.Match(eb.wildcardSubs[i].pattern, event.Topic); ok {
			matchedWildcard = append(matchedWildcard, eb.wildcardSubs[i].channels)
			extraLen += len(eb.wildcardSubs[i].channels)
		}
	}

	total := exactLen + extraLen
	if total == 0 {
		eb.mu.RUnlock()
		return nil
	}

	all := make([]EventChan, exactLen, total)
	copy(all, exact)
	for _, chs := range matchedWildcard {
		all = append(all, chs...)
	}
	eb.mu.RUnlock()

	for _, subscriber := range all {
		eb.safeSend(subscriber, event)
	}
	return nil
}

// rebuildChain 按注册顺序重建中间件链，Use() 和 NewEventBus() 中调用。
// 调用方须持有写锁（eb.mu.Lock）。
func (eb *EventBus) rebuildChain() {
	var h Handler = EventHandlerFunc(eb.dispatch)
	for i := len(eb.middlewares) - 1; i >= 0; i-- {
		h = eb.middlewares[i](h)
	}
	// Fix #3: 原子替换，Publish 侧无锁读不会与此处写产生竞争。
	eb.chain.Store(h)
}

// safeSend recover-guarded send，通道满时丢弃并计数。
func (eb *EventBus) safeSend(subscriber EventChan, event *Event) {
	defer func() { recover() }()
	select {
	case subscriber <- event:
		eb.metrics.IncDispatched(event.Topic)
	default:
		eb.metrics.IncDropped(event.Topic)
		// Fix #8: 改用结构化字段替代 fmt.Sprintf，避免高频路径下的无谓字符串分配。
		logWarn("subscriber channel full, event dropped: topic=%s", event.Topic)
	}
}

// Use 添加中间件
func (eb *EventBus) Use(mw ...Middleware) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.middlewares = append(eb.middlewares, mw...)
	eb.rebuildChain()
}

// Publish 发布事件到内存总线，经中间件链处理后分发给匹配的订阅者。
// Fix #3: 通过 atomic.Value.Load 无锁读取 chain，与 Use 的写操作不再有数据竞争。
func (eb *EventBus) Publish(ctx context.Context, event *Event) {
	handler := eb.chain.Load().(Handler)
	if err := handler.Handle(ctx, event); err != nil {
		logError("event handling failed: %v", err)
	}
}

// PublishByTopic 按主题发布事件，自动创建 Event 对象。
func (eb *EventBus) PublishByTopic(ctx context.Context, topic string, payload any) {
	event := NewEvent(topic, payload)
	eb.Publish(ctx, event)
}

// Subscribe 同步订阅，返回事件通道供用户自行消费。
// ⚠️  注意：此方法返回的 channel 不纳入 activeSubs 追踪，Close() 时会被强制关闭。
// 若需要优雅取消，请改用 SubscribeAsync 或 SubscribeWithPool。
// topic 支持通配符：含 "*" 时启用 path.Match 匹配（* 匹配单层，不跨点号）。
func (eb *EventBus) Subscribe(ctx context.Context, topic string) EventChan {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	ch := make(EventChan, 100)
	if strings.Contains(topic, "*") {
		for i := range eb.wildcardSubs {
			if eb.wildcardSubs[i].pattern == topic {
				eb.wildcardSubs[i].channels = append(eb.wildcardSubs[i].channels, ch)
				return ch
			}
		}
		eb.wildcardSubs = append(eb.wildcardSubs, wildcardEntry{pattern: topic, channels: []EventChan{ch}})
	} else {
		eb.subscribers[topic] = append(eb.subscribers[topic], ch)
	}
	return ch
}

// removeChannel 内部方法：从订阅表中移除指定通道，不关闭通道本身。
// Fix #10: 将原 Deprecated 的公开 Unsubscribe 的清理逻辑下沉为私有方法，
// asyncSubscription / poolSubscription 统一调用此方法，不再依赖 Deprecated 接口。
func (eb *EventBus) removeChannel(topic string, ch EventChan) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if strings.Contains(topic, "*") {
		for i, entry := range eb.wildcardSubs {
			if entry.pattern == topic {
				for j, c := range entry.channels {
					if c == ch {
						eb.wildcardSubs[i].channels = append(entry.channels[:j], entry.channels[j+1:]...)
						if len(eb.wildcardSubs[i].channels) == 0 {
							eb.wildcardSubs = append(eb.wildcardSubs[:i], eb.wildcardSubs[i+1:]...)
						}
						logInfo("wildcard subscription removed: topic=%s", topic)
						return
					}
				}
			}
		}
		return
	}

	if subscribers, ok := eb.subscribers[topic]; ok {
		for i, subscriber := range subscribers {
			if ch == subscriber {
				eb.subscribers[topic] = append(subscribers[:i], subscribers[i+1:]...)
				logInfo("subscription removed: topic=%s", topic)
				return
			}
		}
	}
}

// Unsubscribe 取消订阅，从 topic 中移除指定通道。
// Deprecated: 请使用 Subscription.Unsubscribe()，它会安全等待 worker 退出后关闭通道。
func (eb *EventBus) Unsubscribe(topic string, ch EventChan) {
	eb.removeChannel(topic, ch)
}

// trackSub 注册活跃订阅（由 SubscribeAsync / SubscribeWithPool 自动调用）
func (eb *EventBus) trackSub(s Subscription) {
	eb.subsMu.Lock()
	eb.activeSubs = append(eb.activeSubs, s)
	eb.subsMu.Unlock()
}

// untrackSub 移除活跃订阅（由 Subscription.Unsubscribe 回调）
func (eb *EventBus) untrackSub(s Subscription) {
	eb.subsMu.Lock()
	for i, sub := range eb.activeSubs {
		if sub == s {
			eb.activeSubs = append(eb.activeSubs[:i], eb.activeSubs[i+1:]...)
			break
		}
	}
	eb.subsMu.Unlock()
}

// SetMetrics 注入自定义 Metrics 实现（Prometheus 等）
func (eb *EventBus) SetMetrics(m Metrics) {
	if m != nil {
		eb.metrics = m
	}
}

// Close 优雅关闭总线：
//  1. 遍历所有活跃 Subscription 并调用 Unsubscribe（等待 worker 退出 + close channel）
//  2. 清理精确匹配 map 和通配符列表中残留的通道（带 recover 保护）
func (eb *EventBus) Close() error {
	eb.subsMu.Lock()
	subs := eb.activeSubs
	eb.activeSubs = nil
	eb.subsMu.Unlock()
	for _, sub := range subs {
		sub.Unsubscribe()
	}

	eb.mu.Lock()
	defer eb.mu.Unlock()
	for topic, chs := range eb.subscribers {
		for _, ch := range chs {
			func() {
				defer func() { recover() }()
				close(ch)
			}()
		}
		delete(eb.subscribers, topic)
	}
	for _, entry := range eb.wildcardSubs {
		for _, ch := range entry.channels {
			func() {
				defer func() { recover() }()
				close(ch)
			}()
		}
	}
	eb.wildcardSubs = nil
	return nil
}
