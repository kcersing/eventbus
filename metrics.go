package eventbus

// Metrics 事件总线可观测性接口，调用方可注入 Prometheus 等实现
type Metrics interface {
	// IncPublished 事件发布计数
	IncPublished(topic string)
	// IncDispatched 事件分发成功计数
	IncDispatched(topic string)
	// IncDropped 事件丢弃计数（通道满或降级失败）
	IncDropped(topic string)
	// IncError 事件处理错误计数
	IncError(topic string)
	// IncMQPublished MQ 发布成功/失败计数
	IncMQPublished(topic string, success bool)
	// ObserveHandleDuration 事件处理耗时（微秒）
	ObserveHandleDuration(topic string, micros float64)
	// SetQueueDepth 队列深度快照
	SetQueueDepth(topic string, depth int)
}

// noopMetrics 默认空实现，避免 nil 检查
type noopMetrics struct{}

func (noopMetrics) IncPublished(string)                   {}
func (noopMetrics) IncDispatched(string)                  {}
func (noopMetrics) IncDropped(string)                     {}
func (noopMetrics) IncError(string)                       {}
func (noopMetrics) IncMQPublished(string, bool)           {}
func (noopMetrics) ObserveHandleDuration(string, float64) {}
func (noopMetrics) SetQueueDepth(string, int)             {}
