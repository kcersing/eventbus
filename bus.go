package eventbus

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// EventChan 是订阅者接收事件的通道类型。
type EventChan chan *Event

// Subscription 代表一个活跃订阅，调用 Unsubscribe 可安全取消并释放资源。
// 多次调用是幂等的。
type Subscription interface {
	Unsubscribe()
}

// subscription 是内部订阅记录，id 用于投递回执标识订阅方。
type subscription struct {
	id    string
	topic string
	ch    EventChan
}

// wildcardEntry 将同一通配符 pattern 的所有订阅归组管理。
// segments 是 pattern 按 '.' 拆分后的缓存，供匹配时避免重复 Split。
type wildcardEntry struct {
	pattern  string
	segments []string
	subs     []*subscription
}

// EventBus 内存事件总线。
//
//   - 支持精确主题订阅和通配符订阅：按 '.' 分段，'*' 匹配一个段，
//     段内支持 '?' 与 '[]'（语义同 path.Match）
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
	subscribers  map[string][]*subscription
	wildcardSubs []wildcardEntry
	middlewares  []Middleware
	chain        atomic.Value // stores Handler，原子读写消除 Publish/Use 竞争
	activeSubs   []Subscription
	subsMu       sync.Mutex
	subSeq       atomic.Uint64
}

// New 创建事件总线。
func New() *EventBus {
	eb := &EventBus{
		subscribers: make(map[string][]*subscription),
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

// PublishReport 发布事件并返回每个匹配订阅的投递回执。
//
// 与 Publish 的区别：
//   - 返回每个订阅者的投递结果（queued / dropped）；
//   - 中间件返回错误时不分发，并返回该错误与已产生的回执；
//   - 回执只代表“写入订阅者通道成功”，不代表 Handler 处理完成。
//
// AMQP Listener 使用此方法在记录回执后再 Ack。中间件必须透传 ctx，否则回执
// 收集器会丢失。
func (eb *EventBus) PublishReport(ctx context.Context, event *Event) ([]DeliveryReceipt, error) {
	collector := &receiptCollector{}
	handler := eb.chain.Load().(Handler)
	err := handler.Handle(context.WithValue(ctx, receiptCollectorKey{}, collector), event)
	return collector.receipts, err
}

// Subscribe 返回原始事件通道，由调用方自行管理消费 goroutine。
//
// 与 SubscribeAsync 不同，此方法不追踪订阅生命周期 —— ctx 取消时不会自动清理，
// 调用方需自行协调 goroutine 退出和通道关闭。总线 Close() 时会强制关闭所有残留通道。
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
	for topic, subs := range eb.subscribers {
		for _, sub := range subs {
			closeSafe(sub.ch)
		}
		delete(eb.subscribers, topic)
	}
	for _, entry := range eb.wildcardSubs {
		for _, sub := range entry.subs {
			closeSafe(sub.ch)
		}
	}
	eb.wildcardSubs = nil
	return nil
}

// ── 内部方法 ────────────────────────────────────────────────────────────────
// dispatch
func (eb *EventBus) dispatch(ctx context.Context, event *Event) error {
	collector := receiptCollectorFromContext(ctx)

	eb.mu.RLock()

	exact := eb.subscribers[event.Topic]
	if len(exact) == 0 && len(eb.wildcardSubs) == 0 {
		eb.mu.RUnlock()
		return nil
	}

	// 统计通配符命中的真实订阅，顺带判断是否存在匹配。
	var wildcardHits []*subscription
	if len(eb.wildcardSubs) > 0 {
		topicSegments := strings.Split(event.Topic, ".")
		for i := range eb.wildcardSubs {
			if matchSegments(eb.wildcardSubs[i].segments, topicSegments) {
				wildcardHits = append(wildcardHits, eb.wildcardSubs[i].subs...)
			}
		}
	}

	// 快速路径：单一精确订阅者且无通配符命中。
	if len(exact) == 1 && len(wildcardHits) == 0 {
		sub := exact[0]
		eb.mu.RUnlock()
		eb.deliver(collector, sub, event)
		return nil
	}

	// 复制一份目标列表，避免解锁后被 RemoveChannel 修改底层切片。
	targets := make([]*subscription, 0, len(exact)+len(wildcardHits))
	targets = append(targets, exact...)
	targets = append(targets, wildcardHits...)
	eb.mu.RUnlock()

	for _, sub := range targets {
		eb.deliver(collector, sub, event)
	}
	return nil
}

// deliver 向单个订阅发送事件，并在需要时记录投递回执。
func (eb *EventBus) deliver(collector *receiptCollector, sub *subscription, event *Event) {
	ok := sendSafe(sub.ch, event)
	if collector == nil {
		return
	}

	status := DeliveryQueued
	if !ok {
		status = DeliveryDropped
	}
	collector.add(DeliveryReceipt{
		EventID:           event.Id,
		Topic:             event.Topic,
		SubscriberID:      sub.id,
		SubscriptionTopic: sub.topic,
		Status:            status,
		At:                time.Now(),
	})
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
	sub := &subscription{
		id:    "sub-" + strconv.FormatUint(eb.subSeq.Add(1), 10),
		topic: topic,
		ch:    ch,
	}
	if isWildcardPattern(topic) {
		segments := strings.Split(topic, ".")
		for i := range eb.wildcardSubs {
			if eb.wildcardSubs[i].pattern == topic {
				eb.wildcardSubs[i].subs = append(eb.wildcardSubs[i].subs, sub)
				return
			}
		}
		eb.wildcardSubs = append(eb.wildcardSubs, wildcardEntry{
			pattern:  topic,
			segments: segments,
			subs:     []*subscription{sub},
		})
	} else {
		eb.subscribers[topic] = append(eb.subscribers[topic], sub)
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

	if isWildcardPattern(topic) {
		for i, entry := range eb.wildcardSubs {
			if entry.pattern != topic {
				continue
			}
			for j, sub := range entry.subs {
				if sub.ch != ch {
					continue
				}
				eb.wildcardSubs[i].subs = append(entry.subs[:j], entry.subs[j+1:]...)
				if len(eb.wildcardSubs[i].subs) == 0 {
					eb.wildcardSubs = append(eb.wildcardSubs[:i], eb.wildcardSubs[i+1:]...)
				}
				return
			}
		}
		return
	}

	subs := eb.subscribers[topic]
	for i, sub := range subs {
		if sub.ch == ch {
			eb.subscribers[topic] = append(subs[:i], subs[i+1:]...)
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

// sendSafe 向通道发送事件；通道满或已关闭时返回 false 并记日志。
func sendSafe(ch EventChan, event *Event) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	select {
	case ch <- event:
		return true
	default:
		logWarn("eventbus: channel full, event dropped: topic=%s", event.Topic)
		return false
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
