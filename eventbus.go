package eventbus

import (
	"context"
	"sync"

	"github.com/cloudwego/kitex/pkg/klog"
)

// EventChan 事件通道
type (
	EventChan chan *Event
)

var _ Bus = (*EventBus)(nil)
var _ Publish = (*EventBus)(nil)
var _ Subscribe = (*EventBus)(nil)

// EventBus 事件总线
type EventBus struct {
	mu          sync.RWMutex
	subscribers map[string][]EventChan // 订阅者映射：topic → 多个通道
	middlewares []Middleware           // 中间件
	chain       Handler                // 缓存的中间件
}

// NewEventBus 创建事件总线
func NewEventBus() *EventBus {
	eb := &EventBus{
		subscribers: make(map[string][]EventChan),
		middlewares: []Middleware{},
	}
	eb.rebuildChain() // 初始化链
	return eb
}

// 将事件分发到内存通道
func (eb *EventBus) dispatch(ctx context.Context, event *Event) error {
	eb.mu.RLock()
	subscribers := append([]EventChan{}, eb.subscribers[event.Topic]...)
	eb.mu.RUnlock()
	for _, subscriber := range subscribers {
		select {
		case subscriber <- event: //发送事件
		default:
			klog.Warnf("警告: 主题 %s 的一个订阅者通道已满，丢弃事件。", event.Topic)
		}
	}
	return nil
}

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

// Publish 发布事件到内存总线
func (eb *EventBus) Publish(ctx context.Context, event *Event) {
	if err := eb.chain.Handle(ctx, event); err != nil {
		klog.Errorf("[Error] Handle event failed: %v", err)
	}
}

// PublishByTopic 按主题发布事件（简化版）
func (eb *EventBus) PublishByTopic(ctx context.Context, topic string, payload any) {
	event := NewEvent(topic, payload)
	eb.Publish(ctx, event)
}

// Subscribe 订阅事件 (同步)
func (eb *EventBus) Subscribe(topic string) EventChan {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	ch := make(EventChan, 100)
	eb.subscribers[topic] = append(eb.subscribers[topic], ch)
	klog.Infof("[Subscribe] 已订阅主题: %s", topic)
	return ch
}

// Unsubscribe 取消订阅 (Deprecated: use Subscription.Unsubscribe() instead)
func (eb *EventBus) Unsubscribe(topic string, ch EventChan) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if subscribers, ok := eb.subscribers[topic]; ok {
		for i, subscriber := range subscribers {
			if ch == subscriber {
				eb.subscribers[topic] = append(subscribers[:i], subscribers[i+1:]...)
				// 只有在通道确认不再被任何goroutine使用时才能安全关闭
				// close(ch) 
				klog.Infof("已取消订阅主题: %s", topic)
				return
			}
		}
	}
}

// --- 异步订阅与资源管理 ---

type asyncSubscription struct {
	eb    *EventBus
	topic string
	ch    EventChan
	ctx   context.Context
	cancel context.CancelFunc
}

func (s *asyncSubscription) Unsubscribe() {
	s.cancel()
	s.eb.Unsubscribe(s.topic, s.ch)
	close(s.ch)
	klog.Infof("[Unsubscribe] 异步订阅已取消, topic=%s", s.topic)
}

// SubscribeAsync 异步订阅：用户只需提供 handler，无需自己管理协程
func (eb *EventBus) SubscribeAsync(topic string, handler Handler, concurrency int) Subscription {
	ch := eb.Subscribe(topic)
	
	ctx, cancel := context.WithCancel(context.Background())
	
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case event, ok := <-ch:
					if !ok {
						return
					}
					eb.safeHandle(topic, handler, event)
				}
			}
		}()
	}
	
	// Goroutine to wait for all handlers to finish before closing channel
	go func(){
		wg.Wait()
	}()

	return &asyncSubscription{
		eb:    eb,
		topic: topic,
		ch:    ch,
		ctx: ctx,
		cancel: cancel,
	}
}

func (eb *EventBus) safeHandle(topic string, handler Handler, event *Event) {
	defer func() {
		if r := recover(); r != nil {
			klog.Errorf("[Panic Recover] Topic: %s, Error: %v", topic, r)
		}
	}()
	if err := handler.Handle(context.Background(), event); err != nil {
		klog.Errorf("[Error] Handle event failed: %v", err)
	}
}

// --- 消费者池订阅 ---

type poolSubscription struct {
	eb *EventBus
	topic string
	ch EventChan
	pool *ConsumerPool
	cancel context.CancelFunc
}

func (s *poolSubscription) Unsubscribe() {
	s.cancel() // 停止转发goroutine
	s.pool.Stop() // 停止消费者池
	s.eb.Unsubscribe(s.topic, s.ch) // 从EventBus中移除通道
	close(s.ch)
	klog.Infof("[Unsubscribe] 消费者池订阅已取消, topic=%s", s.topic)
}

// SubscribeWithPool 使用消费者池订阅事件
func (eb *EventBus) SubscribeWithPool(topic string, handler Handler, workerNum int32, opts ...func(*PoolOptions)) Subscription {
	pool := NewConsumerPool(topic, handler, workerNum, opts...)
	pool.Start()

	ch := eb.Subscribe(topic)
	ctx, cancel := context.WithCancel(context.Background())

	// 启动一个转发goroutine，将事件从EventBus的通道转发到消费者池
	go func() {
		defer func() {
			klog.Infof("转发协程已停止, topic=%s", topic)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-ch:
				if !ok { // 通道被关闭
					return
				}
				pool.Consume(event)
			}
		}
	}()

	return &poolSubscription{
		eb: eb,
		topic: topic,
		ch: ch,
		pool: pool,
		cancel: cancel,
	}
}

func (eb *EventBus) Close() error {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	// 关闭所有订阅通道
	for topic, chs := range eb.subscribers {
		for _, ch := range chs {
			close(ch)
		}
		delete(eb.subscribers, topic)
	}
	return nil
}
