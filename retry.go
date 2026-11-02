package eventbus

import (
	"context"
	"errors"
)

// RetryDecision 是消费者对一次失败的处理决策。
type RetryDecision int

const (
	// Retry 继续重试（或重新入队）。
	Retry RetryDecision = iota
	// DeadLetter 不再重试，进入死信/失败处理。
	DeadLetter
	// Drop 直接确认并丢弃（明确不需要重试，也不进入死信）。
	Drop
)

// RetryClassifier 根据事件与错误决定重试策略。
//
// 返回 DeadLetter 时，消费者应进入死信/失败回调；返回 Drop 时应确认并丢弃；
// 返回 Retry 时按剩余重试次数继续。实现必须是并发安全的。
type RetryClassifier func(ctx context.Context, event *Event, err error) RetryDecision

// DefaultRetryClassifier 是默认策略：标记为 ErrUnrecoverable 的错误进入死信，
// 其他错误重试。
func DefaultRetryClassifier(_ context.Context, _ *Event, err error) RetryDecision {
	if errors.Is(err, ErrUnrecoverable) {
		return DeadLetter
	}
	return Retry
}
