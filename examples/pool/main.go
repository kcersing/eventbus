// Package main 演示消费者池：并发 worker、超时、指数退避重试、死信回调。
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/kcersing/eventbus"
	"github.com/kcersing/eventbus/ext/pool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	bus := eventbus.New()
	defer bus.Close()

	var (
		mu       sync.Mutex
		attempts = map[any]int{}
	)

	sub := pool.SubscribeWithPool(ctx, bus, "job.created",
		eventbus.HandlerFunc(func(ctx context.Context, e *eventbus.Event) error {
			mu.Lock()
			attempts[e.Payload]++
			n := attempts[e.Payload]
			mu.Unlock()

			// 99 是“毒消息”，永远失败，用于演示死信回调。
			if e.Payload == 99 {
				return fmt.Errorf("poison event")
			}
			// 前两次失败，演示指数退避重试。
			if n < 2 {
				return fmt.Errorf("transient failure (attempt %d)", n)
			}

			fmt.Printf("[worker] payload=%v attempts=%d\n", e.Payload, n)
			return nil
		}),
		4,
		pool.WithQueueSize(100),
		pool.WithMaxRetries(3),
		pool.WithRetryBackoff(50*time.Millisecond),
		pool.WithHandlerTimeout(2*time.Second),
		pool.WithDeadLetterFunc(func(e *eventbus.Event) {
			fmt.Printf("[dead-letter] topic=%s payload=%v event=%s\n", e.Topic, e.Payload, e.Id)
		}),
	)
	defer sub.Unsubscribe()

	for i := 0; i < 4; i++ {
		bus.PublishTopic(ctx, "job.created", i)
	}
	bus.PublishTopic(ctx, "job.created", 99)

	// 等待重试链执行完；生产环境应在关闭流程中等待/取消。
	time.Sleep(1200 * time.Millisecond)
	fmt.Println("done")
}
