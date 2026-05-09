package eventbus

import "errors"

var (
	// ErrQueueFull 表示消费者池的队列已满
	ErrQueueFull = errors.New("consumer pool queue is full")
)
