package eventbus

import (
	"context"
	"path"
	"strings"
	"sync"

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
	pattern  string        // 通配符模式，如 "order.*"
	channels []EventChan   // 该模式下的所有订阅通道
}

// EventBus 内存事件总线，支持 topic 订阅、中间件链和通配符匹配。
// 线程安全，可并发读写。
type EventBus struct {
	mu           sync.RWMutex
	subscribers  map[string][]EventChan // 精确匹配订阅
	wildcardSubs []wildcardEntry        // 通配符订阅（* 匹配单层）
	middlewares  []Middleware           // 中间件
	chain        Handler                // 缓存的中间件
	activeSubs   []Subscription         // 活跃订阅列表（用于 Close 时统一取消）
	subsMu       sync.Mutex
	metrics      Metrics                // 可观测性接口
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
// 常规路径：先统计总数 → 精确容量分配 → 拷贝 → 释放锁 → 遍历发送。
func (eb *EventBus) dispatch(ctx context.Context, event *Event) error {
	eb.mu.RLock()
	exact := eb.subscribers[event.Topic]
	exactLen := len(exact)

	// 短路径：只有单个精确订阅者，无通配符，零分配
	if exactLen == 1 && len(eb.wildcardSubs) == 0 {
		subscriber := exact[0]
		eb.mu.RUnlock()
		select {
		case subscriber <- event:
			eb.metrics.IncDispatched(event.Topic)
		default:
			eb.metrics.IncDropped(event.Topic)
			logWarn("警告: 主题 %s 的订阅者通道已满，丢弃事件。", event.Topic)
		}
		return nil
	}

	// 常规路径：统计总数，一次性分配精确容量
	total := exactLen
	for _, entry := range eb.wildcardSubs {
		if ok, _ := path.Match(entry.pattern, event.Topic); ok {
			total += len(entry.channels)
		}
	}
	if total == 0 {
		eb.mu.RUnlock()
		return nil
	}
	all := make([]EventChan, 0, total)
	all = append(all, exact...)
	for _, entry := range eb.wildcardSubs {
		if ok, _ := path.Match(entry.pattern, event.Topic); ok {
			all = append(all, entry.channels...)
		}
	}
	eb.mu.RUnlock()

	for _, subscriber := range all {
		select {
		case subscriber <- event:
			eb.metrics.IncDispatched(event.Topic)
		default:
			eb.metrics.IncDropped(event.Topic)
			logWarn("警告: 主题 %s 的一个订阅者通道已满，丢弃事件。", event.Topic)
		}
	}
	return nil
}

// rebuildChain 按注册顺序重建中间件链，Use() 和 NewEventBus() 中调用。
func (eb *EventBus) rebuildChain() {
	var h Handler = EventHandlerFunc(eb.dispatch)
	for i := len(eb.middlewares) - 1; i >= 0; i-- {
		h = eb.middlewares[i](h)
	}
	eb.chain = h
}

// Use 添加中间件
func (eb *EventBus) Use(mw ...Middleware) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.middlewares = append(eb.middlewares, mw...)
	eb.rebuildChain()
}

// Publish 发布事件到内存总线，经中间件链处理后分发给匹配的订阅者。
func (eb *EventBus) Publish(ctx context.Context, event *Event) {
	if err := eb.chain.Handle(ctx, event); err != nil {
		logError("[Error] Handle event failed: %v", err)
	}
}

// PublishByTopic 按主题发布事件，自动创建 Event 对象。
func (eb *EventBus) PublishByTopic(ctx context.Context, topic string, payload any) {
	event := NewEvent(topic, payload)
	eb.Publish(ctx, event)
}

// Subscribe 同步订阅，返回事件通道供用户自行消费。
// topic 支持通配符：含 "*" 时启用 path.Match 匹配（* 匹配单层，不跨点号）。
func (eb *EventBus) Subscribe(ctx context.Context, topic string) EventChan {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	ch := make(EventChan, 100)
	if strings.Contains(topic, "*") {
		// 通配符订阅：合并到已有 pattern 或新建 entry
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

// Unsubscribe 取消订阅，从 topic 中移除指定通道。
// Deprecated: 请使用 Subscription.Unsubscribe()，它会安全等待 worker 退出后关闭通道。
func (eb *EventBus) Unsubscribe(topic string, ch EventChan) {
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
						logInfo("已取消通配符订阅主题: %s", topic)
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
				// 只有在通道确认不再被任何goroutine使用时才能安全关闭
				// close(ch)
				logInfo("已取消订阅主题: %s", topic)
				return
			}
		}
	}
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
	// 1. 取消所有活跃订阅
	eb.subsMu.Lock()
	subs := eb.activeSubs
	eb.activeSubs = nil
	eb.subsMu.Unlock()
	for _, sub := range subs {
		sub.Unsubscribe()
	}

	// 2. 清理精确匹配残留 channel
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
	// 清理通配符残留 channel
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
