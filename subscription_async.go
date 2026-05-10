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
		// Fix #10: 调用私有方法 removeChannel，不再依赖 Deprecated 的公开 Unsubscribe。
		s.eb.removeChannel(s.topic, s.ch)
		close(s.ch)
		s.eb.untrackSub(s)
		logInfo("async subscription cancelled: topic=%s", s.topic)
	})
}

// SubscribeAsync 异步订阅，自动启动 concurrency 个 goroutine 消费事件。
// 返回的 Subscription 可通过 Unsubscribe 安全取消（幂等，可重复调用）。
//
// Fix #1: 额外启动一个监听 goroutine，当父 ctx 被外部取消时自动调用 Unsubscribe，
// 防止父 ctx 取消但 Unsubscribe 未被调用时出现 channel 和 goroutine 泄漏。
func (eb *EventBus) SubscribeAsync(ctx context.Context, topic string, handler Handler, concurrency int) Subscription {
	ch := eb.Subscribe(ctx, topic)

	subCtx, cancel := context.WithCancel(ctx)

	sub := &asyncSubscription{
		eb:     eb,
		topic:  topic,
		ch:     ch,
		ctx:    subCtx,
		cancel: cancel,
	}

	for i := 0; i < concurrency; i++ {
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
					eb.safeHandle(subCtx, topic, handler, event)
				}
			}
		}()
	}

	// Fix #1: 监听父 ctx，父 ctx 取消时触发 Unsubscribe 做完整清理。
	// 使用独立 goroutine 而非在 worker 里处理，以避免与 wg.Wait 产生死锁。
	go func() {
		select {
		case <-ctx.Done():
			// 父 ctx 被取消，执行完整清理（幂等，与手动调用 Unsubscribe 不冲突）
			sub.Unsubscribe()
		case <-subCtx.Done():
			// Unsubscribe 已被手动调用，监听 goroutine 正常退出
		}
	}()

	eb.trackSub(sub)
	return sub
}

// safeHandle 带 panic recover 和 metrics 的事件处理包装。
func (eb *EventBus) safeHandle(ctx context.Context, topic string, handler Handler, event *Event) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			eb.metrics.IncError(topic)
			logError("panic recovered in handler: topic=%s panic=%v", topic, r)
		}
	}()
	if err := handler.Handle(ctx, event); err != nil {
		eb.metrics.IncError(topic)
		logError("event handling failed: topic=%s error=%v", topic, err)
	}
	eb.metrics.ObserveHandleDuration(topic, float64(time.Since(start).Microseconds()))
}
