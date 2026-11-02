package eventbus

import (
	"errors"
	"fmt"
)

// ErrChannelFull 表示订阅者通道已满，事件被丢弃。
var ErrChannelFull = errors.New("eventbus: subscriber channel full")

// ErrUnrecoverable 标记“重试无意义”的 Handler 错误。
//
// 例如消息格式错误、业务数据不存在、参数校验失败等。消费者/消费者池
// 应据此直接进入死信或丢弃，而不是反复重试。
var ErrUnrecoverable = errors.New("eventbus: unrecoverable")

// Unrecoverable 包装一个不可恢复错误，供 errors.Is/As 判断。
func Unrecoverable(err error) error {
	if err == nil {
		return ErrUnrecoverable
	}
	return fmt.Errorf("%w: %w", ErrUnrecoverable, err)
}

// IsUnrecoverable 判断错误是否被标记为不可恢复。
func IsUnrecoverable(err error) bool {
	return errors.Is(err, ErrUnrecoverable)
}
