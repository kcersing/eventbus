package eventbus

import "time"

// Config 可配置项，用于调整池和处理器行为
type Config struct {
	QueueSize      int           // ConsumerPool 队列大小
	HandlerTimeout time.Duration // 每个 handler 的最大处理时长（0 表示无超时）
}

// DefaultConfig 默认配置
var DefaultConfig = &Config{
	QueueSize:      1000,
	HandlerTimeout: 0,
}
