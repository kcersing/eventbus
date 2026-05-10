package eventbus

import (
	"context"
	"path"
	"strings"
	"sync"
	"sync/atomic"
)

// EventChan 是订阅者接收事件的通道类型。
type EventChan chan *Event

// Subscription 代表一个活跃订阅，调用 Unsubscribe 可安全取消并释放资源。
// 多次调用是幂等的。
type Subscription interface {
	Unsubscribe()
}

// wildcardEntry 将同一通配符 pattern 的所有订阅通道归组管理。
type wildcardEntry struct {
	pattern  string
	channels []EventChan
}

// EventBus 内存事件总线。
//
//   - 支持精确主题订阅和通配符订阅（* 匹配单层，语义同 path.Match）
//   - 支持中间件链（Use）
//   - 线程安全
//
// 快速上手：
//
//	bus := eventbus.New()
//	sub := bus.SubscribeAsync(ctx, "order.created", handler, 2)
//	defer sub.Unsubscribe()
//	bus.Publish(ctx, eventbus.NewEvent("order.created", payload))
type EventBus struct {
	mu           sync.RWMutex
	subscribers  map[string][]EventChan
	wildcardSubs []wildcardEntry
	middlewares  []Middleware
	chain        atomic.Value // stores Handler，原子读写消除 Publish/Use 竞争
	activeSubs   []Subscription
	subsMu       sync.Mutex
}

// New 创建事件总线。
func New() *EventBus {
	eb := &EventBus{
		subscribers: make(map[string][]EventChan),
	}
	eb.storeChain(HandlerFunc(eb.dispatch))
	return eb
}

// Use 注册中间件，按注册顺序执行。可在任意时刻调用，立即生效。
func (eb *EventBus) Use(mw ...Middleware) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.middlewares = append(eb.middlewares, mw...)
	eb.rebuildChain()
}

// Publish 发布事件，经中间件链处理后分发给所有匹配的订阅者。
func (eb *EventBus) Publish(ctx context.Context, event *Event) {
	handler := eb.chain.Load().(Handler)
	if err := handler.Handle(ctx, event); err != nil {
		logError("eventbus: publish error: %v", err)
	}
}

// PublishTopic 按主题发布事件的便捷方法，内部自动构造 Event。
func (eb *EventBus) PublishTopic(ctx context.Context, topic string, payload any) {
	eb.Publish(ctx, NewEvent(topic, payload))
}

// Subscribe 返回原始事件通道，由调用方自行管理消费 goroutine。
//
// ⚠️  此方法返回的通道不被总线追踪，Close() 时会被强制关闭。
// 推荐使用 SubscribeAsync 以获得完整的生命周期管理。
func (eb *EventBus) Subscribe(ctx context.Context, topic string) EventChan {
	ch := make(EventChan, 64)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.addChannel(topic, ch)
	return ch
}

// SubscribeAsync 异步订阅：自动启动 concurrency 个 goroutine 消费事件。
// 返回的 Subscription 可通过 Unsubscribe 安全取消（幂等）。
//
// 当父 ctx 被取消时，订阅会自动清理，无需手动调用 Unsubscribe。
func (eb *EventBus) SubscribeAsync(ctx context.Context, topic string, handler Handler, concurrency int) Subscription {
	ch := eb.Subscribe(ctx, topic)
	subCtx, cancel := context.WithCancel(ctx)

	sub := &asyncSub{
		eb:     eb,
		topic:  topic,
		ch:     ch,
		cancel: cancel,
	}

	for range concurrency {
		sub.wg.Add(1)
		go func() {
			defer sub.wg.Done()
			for {
				select {
				case <-subCtx.Done():
					return
				case event, ok := <-ch:
					if !ok {
						return
					}
					safeHandle(subCtx, handler, event)
				}
			}
		}()
	}

	// 父 ctx 取消时自动清理，防止 goroutine/channel 泄漏
	go func() {
		select {
		case <-ctx.Done():
			sub.Unsubscribe()
		case <-subCtx.Done():
			// 手动调用了 Unsubscribe，正常退出
		}
	}()

	eb.trackSub(sub)
	return sub
}

// Close 优雅关闭总线：取消所有活跃订阅，再清理残留通道。
func (eb *EventBus) Close() error {
	eb.subsMu.Lock()
	subs := eb.activeSubs
	eb.activeSubs = nil
	eb.subsMu.Unlock()

	for _, s := range subs {
		s.Unsubscribe()
	}

	eb.mu.Lock()
	defer eb.mu.Unlock()

	// 关闭残留（通过 Subscribe 直接获取的）通道
	for topic, chs := range eb.subscribers {
		for _, ch := range chs {
			closeSafe(ch)
		}
		delete(eb.subscribers, topic)
	}
	for _, entry := range eb.wildcardSubs {
		for _, ch := range entry.channels {
			closeSafe(ch)
		}
	}
	eb.wildcardSubs = nil
	return nil
}

// ── 内部方法 ────────────────────────────────────────────────────────────────

func (eb *EventBus) dispatch(ctx context.Context, event *Event) error {
	eb.mu.RLock()

	exact := eb.subscribers[event.Topic]
	if len(exact) == 0 && len(eb.wildcardSubs) == 0 {
		eb.mu.RUnlock()
		return nil
	}

	// 快速路径：单一精确订阅者，无通配符
	if len(exact) == 1 && len(eb.wildcardSubs) == 0 {
		ch := exact[0]
		eb.mu.RUnlock()
		sendSafe(ch, event)
		return nil
	}

	// 统计通配符命中的真实 channel 数，精确分配切片
	var matched [][]EventChan
	extraLen := 0
	for i := range eb.wildcardSubs {
		if ok, _ := path.Match(eb.wildcardSubs[i].pattern, event.Topic); ok {
			matched = append(matched, eb.wildcardSubs[i].channels)
			extraLen += len(eb.wildcardSubs[i].channels)
		}
	}

	all := make([]EventChan, len(exact), len(exact)+extraLen)
	copy(all, exact)
	for _, chs := range matched {
		all = append(all, chs...)
	}
	eb.mu.RUnlock()

	for _, ch := range all {
		sendSafe(ch, event)
	}
	return nil
}

func (eb *EventBus) rebuildChain() {
	var h Handler = HandlerFunc(eb.dispatch)
	for i := len(eb.middlewares) - 1; i >= 0; i-- {
		h = eb.middlewares[i](h)
	}
	eb.storeChain(h)
}

func (eb *EventBus) storeChain(h Handler) {
	eb.chain.Store(h)
}

// addChannel 将 ch 注册到订阅表（调用方须持有写锁）。
func (eb *EventBus) addChannel(topic string, ch EventChan) {
	if strings.Contains(topic, "*") {
		for i := range eb.wildcardSubs {
			if eb.wildcardSubs[i].pattern == topic {
				eb.wildcardSubs[i].channels = append(eb.wildcardSubs[i].channels, ch)
				return
			}
		}
		eb.wildcardSubs = append(eb.wildcardSubs, wildcardEntry{pattern: topic, channels: []EventChan{ch}})
	} else {
		eb.subscribers[topic] = append(eb.subscribers[topic], ch)
	}
}

// RemoveChannel 从订阅表中移除指定通道（不关闭通道本身）。
// 扩展包（如 ext/pool）在实现自定义 Subscription 时使用此方法。
func (eb *EventBus) RemoveChannel(topic string, ch EventChan) {
	eb.removeChannel(topic, ch)
}

// removeChannel 内部实现。
func (eb *EventBus) removeChannel(topic string, ch EventChan) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if strings.Contains(topic, "*") {
		for i, entry := range eb.wildcardSubs {
			if entry.pattern != topic {
				continue
			}
			for j, c := range entry.channels {
				if c != ch {
					continue
				}
				eb.wildcardSubs[i].channels = append(entry.channels[:j], entry.channels[j+1:]...)
				if len(eb.wildcardSubs[i].channels) == 0 {
					eb.wildcardSubs = append(eb.wildcardSubs[:i], eb.wildcardSubs[i+1:]...)
				}
				return
			}
		}
		return
	}

	chs := eb.subscribers[topic]
	for i, c := range chs {
		if c == ch {
			eb.subscribers[topic] = append(chs[:i], chs[i+1:]...)
			return
		}
	}
}

func (eb *EventBus) trackSub(s Subscription) {
	eb.subsMu.Lock()
	eb.activeSubs = append(eb.activeSubs, s)
	eb.subsMu.Unlock()
}

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

// ── 辅助函数 ────────────────────────────────────────────────────────────────

// sendSafe 向通道发送事件；通道满时丢弃并记日志。
func sendSafe(ch EventChan, event *Event) {
	defer func() { recover() }()
	select {
	case ch <- event:
	default:
		logWarn("eventbus: channel full, event dropped: topic=%s", event.Topic)
	}
}

// closeSafe 关闭通道，忽略重复关闭的 panic。
func closeSafe(ch EventChan) {
	defer func() { recover() }()
	close(ch)
}

// safeHandle 带 panic recover 的事件处理包装。
func safeHandle(ctx context.Context, handler Handler, event *Event) {
	defer func() {
		if r := recover(); r != nil {
			logError("eventbus: handler panic: topic=%s panic=%v", event.Topic, r)
		}
	}()
	if err := handler.Handle(ctx, event); err != nil {
		logError("eventbus: handler error: topic=%s error=%v", event.Topic, err)
	}
}

// ── asyncSub ────────────────────────────────────────────────────────────────

type asyncSub struct {
	eb     *EventBus
	topic  string
	ch     EventChan
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

func (s *asyncSub) Unsubscribe() {
	s.once.Do(func() {
		s.cancel()
		s.wg.Wait()
		s.eb.removeChannel(s.topic, s.ch)
		closeSafe(s.ch)
		s.eb.untrackSub(s)
		logInfo("eventbus: unsubscribed: topic=%s", s.topic)
	})
}
