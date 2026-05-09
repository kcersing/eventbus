package eventbus

import (
	"context"
	"sync"
	"time"

)

// asyncSubscription SubscribeAsync 返回的订阅实现。
// cancel → wg.Wait 等待所有 worker 退出 → 从 bus 移除 → close channel。
type asyncSubscription struct {
	eb     *EventBus
	topic  string
	ch     EventChan
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

func (s *asyncSubscription) Unsubscribe() {
	s.once.Do(func() {
		s.cancel()
		s.wg.Wait()
		s.eb.Unsubscribe(s.topic, s.ch)
		close(s.ch)
		s.eb.untrackSub(s)
		logInfo("[Unsubscribe] 异步订阅已取消, topic=%s", s.topic)
	})
}

// SubscribeAsync 异步订阅，自动启动 concurrency 个 goroutine 消费事件。
// 返回的 Subscription 可通过 Unsubscribe 安全取消（幂等，可重复调用）。
func (eb *EventBus) SubscribeAsync(ctx context.Context, topic string, handler Handler, concurrency int) Subscription {
	ch := eb.Subscribe(ctx, topic)

	ctx, cancel := context.WithCancel(ctx)

	sub := &asyncSubscription{
		eb:     eb,
		topic:  topic,
		ch:     ch,
		ctx:    ctx,
		cancel: cancel,
	}

	for i := 0; i < concurrency; i++ {
		sub.wg.Add(1)
		go func() {
			defer sub.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case event, ok := <-ch:
					if !ok {
						return
					}
					eb.safeHandle(ctx, topic, handler, event)
				}
			}
		}()
	}

	eb.trackSub(sub)
	return sub
}

// safeHandle 带 panic recover 和 metrics 的事件处理包装。
func (eb *EventBus) safeHandle(ctx context.Context, topic string, handler Handler, event *Event) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			eb.metrics.IncError(topic)
			logError("[Panic Recover] Topic: %s, Error: %v", topic, r)
		}
	}()
	if err := handler.Handle(ctx, event); err != nil {
		eb.metrics.IncError(topic)
		logError("[Error] Handle event failed: %v", err)
	}
	eb.metrics.ObserveHandleDuration(topic, float64(time.Since(start).Microseconds()))
}
