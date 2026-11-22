// Package main 演示服务端如何使用投递回执、幂等验证和处理补偿。
//
// 这里用内存事件总线模拟 MQ 消费侧：
//   - 发布时记录每个订阅者的投递回执；
//   - 业务 Handler 用 Event.Id 做幂等；
//   - 第一次处理失败的事件进入补偿队列，延迟重试；
//   - 重试与重复投递都会被幂等逻辑拦住，不会重复执行业务。
//
// 真实接入 AMQP 时，把 amqp.Listener 配置上 WithDeliveryRecorder，
// 它会在 Record 成功后才 Ack（见 examples/amqp-bridge）。
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kcersing/eventbus"
)

// receiptRecorder 记录每个订阅者的投递回执；生产环境应替换为持久化实现。
type receiptRecorder struct {
	mu       sync.Mutex
	receipts []eventbus.DeliveryReceipt
}

func (r *receiptRecorder) Record(_ context.Context, receipts []eventbus.DeliveryReceipt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.receipts = append(r.receipts, receipts...)
	return nil
}

func (r *receiptRecorder) snapshot() []eventbus.DeliveryReceipt {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]eventbus.DeliveryReceipt, len(r.receipts))
	copy(cp, r.receipts)
	return cp
}

// idempotencyStore 用 Event.Id 做幂等，并记录尝试次数以支持补偿重试。
type idempotencyStore struct {
	mu        sync.Mutex
	processed map[string]bool
	attempts  map[string]int
}

func newIdempotencyStore() *idempotencyStore {
	return &idempotencyStore{
		processed: make(map[string]bool),
		attempts:  make(map[string]int),
	}
}

// begin 返回是否应该处理该事件，以及当前尝试次数。
func (s *idempotencyStore) begin(id string) (firstAttempt bool, attempt int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.processed[id] {
		return false, s.attempts[id]
	}
	s.attempts[id]++
	return true, s.attempts[id]
}

func (s *idempotencyStore) markProcessed(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processed[id] = true
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	bus := eventbus.New()
	defer bus.Close()
	defer cancel()

	recorder := &receiptRecorder{}
	store := newIdempotencyStore()
	compensation := make(chan *eventbus.Event, 16)

	// 业务订阅者：幂等 + 模拟第一次失败。
	businessSub := bus.SubscribeAsync(ctx, "order.created", eventbus.HandlerFunc(
		func(ctx context.Context, e *eventbus.Event) error {
			first, attempt := store.begin(e.Id)
			if !first {
				fmt.Printf("[business] duplicate event %s ignored\n", e.Id)
				return nil
			}

			if e.Payload == "fail" && attempt == 1 {
				fmt.Printf("[business] event %s failed on attempt %d, enqueue compensation\n", e.Id, attempt)
				select {
				case compensation <- e:
				default:
				}
				return fmt.Errorf("simulated failure")
			}

			store.markProcessed(e.Id)
			fmt.Printf("[business] processed event %s payload=%v attempt=%d\n", e.Id, e.Payload, attempt)
			return nil
		}), 2)
	defer businessSub.Unsubscribe()

	// 审计订阅者：不参与幂等，只观察 fanout。
	auditSub := bus.SubscribeAsync(ctx, "order.*", eventbus.HandlerFunc(
		func(ctx context.Context, e *eventbus.Event) error {
			fmt.Printf("[audit] saw topic=%s event=%s\n", e.Topic, e.Id)
			return nil
		}), 1)
	defer auditSub.Unsubscribe()

	// 补偿 worker：延迟重投失败事件；重投同样走回执记录。
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-compensation:
				fmt.Printf("[compensation] retrying event %s in 200ms\n", e.Id)
				select {
				case <-ctx.Done():
					return
				case <-time.After(200 * time.Millisecond):
				}
				publish(ctx, bus, recorder, e)
			}
		}
	}()

	okEvent := eventbus.NewEvent("order.created", "ok")
	publish(ctx, bus, recorder, okEvent)
	publish(ctx, bus, recorder, okEvent) // 重复投递：业务侧应幂等

	failEvent := eventbus.NewEvent("order.created", "fail")
	publish(ctx, bus, recorder, failEvent) // 第一次失败，补偿 worker 会重试

	time.Sleep(800 * time.Millisecond)

	// 汇总投递回执：只能证明“已投递”，不能证明“已完成”。
	all := recorder.snapshot()
	fmt.Printf("\n[summary] delivery receipts=%d (queued/dropped are delivery facts, not completion)\n", len(all))
	for _, r := range all {
		fmt.Printf("[summary] event=%s subscriber=%s topic=%s status=%s\n",
			r.EventID, r.SubscriberID, r.SubscriptionTopic, r.Status)
	}
	fmt.Println("done")
}

func publish(ctx context.Context, bus *eventbus.EventBus, recorder *receiptRecorder, e *eventbus.Event) {
	receipts, err := bus.PublishReport(ctx, e)
	if err != nil {
		fmt.Printf("[publisher] event %s publish failed: %v\n", e.Id, err)
		return
	}
	if err := recorder.Record(ctx, receipts); err != nil {
		fmt.Printf("[publisher] event %s record failed: %v\n", e.Id, err)
		return
	}
	fmt.Printf("[publisher] event=%s topic=%s receipts=%d\n", e.Id, e.Topic, len(receipts))
}
