package eventbus

import (
	"context"
	"sync"
)

// poolSubscription SubscribeWithPool 返回的订阅实现。
// 包含一个转发 goroutine 将 EventBus 通道的事件转发到 ConsumerPool。
type poolSubscription struct {
	eb     *EventBus
	topic  string
	ch     EventChan
	pool   *ConsumerPool
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

func (s *poolSubscription) Unsubscribe() {
	s.once.Do(func() {
		s.cancel()                         // 停止转发 goroutine
		s.wg.Wait()                        // 等待转发 goroutine 退出
		s.pool.Stop()                      // 停止消费者池（cancel + 等待 worker 退出）
		// Fix #10: 调用私有方法 removeChannel，不再依赖 Deprecated 的公开 Unsubscribe。
		s.eb.removeChannel(s.topic, s.ch) // 从 EventBus 中移除通道
		close(s.ch)
		s.eb.untrackSub(s)
		logInfo("pool subscription cancelled: topic=%s", s.topic)
	})
}

// SubscribeWithPool 使用 ConsumerPool 订阅事件，支持重试/超时/死信等高级配置。
// 通过 opts 参数可设置 MaxRetries、RetryBackoff、DeadLetterFunc 等。
func (eb *EventBus) SubscribeWithPool(ctx context.Context, topic string, handler Handler, workerNum int32, opts ...func(*PoolOptions)) Subscription {
	pool := NewConsumerPool(topic, handler, workerNum, opts...)
	pool.SetMetrics(eb.metrics)
	pool.Start()

	ch := eb.Subscribe(ctx, topic)
	ctx, cancel := context.WithCancel(ctx)

	sub := &poolSubscription{
		eb:     eb,
		topic:  topic,
		ch:     ch,
		pool:   pool,
		cancel: cancel,
	}

	// 启动转发 goroutine，将事件从 EventBus 的通道转发到消费者池
	sub.wg.Add(1)
	go func() {
		defer sub.wg.Done()
		defer func() {
			logInfo("forwarder goroutine stopped: topic=%s", topic)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-ch:
				if !ok {
					return
				}
				pool.Consume(event)
			}
		}
	}()

	eb.trackSub(sub)
	return sub
}
