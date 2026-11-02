package eventbus

import (
	"context"
	"time"
)

// DeliveryStatus 描述一条事件对某个订阅者的投递结果。
//
// 注意：投递成功只代表事件已写入订阅者通道，不代表 Handler 已开始或已完成处理。
type DeliveryStatus int

const (
	// DeliveryQueued 表示事件已成功写入订阅者通道。
	DeliveryQueued DeliveryStatus = iota + 1
	// DeliveryDropped 表示订阅者通道已满或已关闭，事件未入队。
	DeliveryDropped
)

func (s DeliveryStatus) String() string {
	switch s {
	case DeliveryQueued:
		return "queued"
	case DeliveryDropped:
		return "dropped"
	default:
		return "unknown"
	}
}

// DeliveryReceipt 是事件对单个订阅者的一条投递回执。
//
// 回执只能证明“已投递到该订阅者的通道”；是否处理成功必须由订阅方另行回执，
// 库不会根据投递回执推断处理完成。
type DeliveryReceipt struct {
	EventID           string
	Topic             string
	SubscriberID      string
	SubscriptionTopic string
	Status            DeliveryStatus
	At                time.Time
}

// DeliveryRecorder 记录投递回执。实现可以是日志、内存、数据库或消息队列。
//
// 使用 AMQP Listener 时，只有 Record 返回 nil 才会 Ack 消息；返回错误时调用方
// 不应确认消息，从而保证“Ack 不早于投递记录”。
type DeliveryRecorder interface {
	Record(ctx context.Context, receipts []DeliveryReceipt) error
}

// receiptCollector 收集一次 PublishReport 产生的投递回执。
type receiptCollector struct {
	receipts []DeliveryReceipt
}

func (c *receiptCollector) add(r DeliveryReceipt) {
	c.receipts = append(c.receipts, r)
}

type receiptCollectorKey struct{}

func receiptCollectorFromContext(ctx context.Context) *receiptCollector {
	c, _ := ctx.Value(receiptCollectorKey{}).(*receiptCollector)
	return c
}
