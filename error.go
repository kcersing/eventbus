package eventbus

import "errors"

// ErrChannelFull 表示订阅者通道已满，事件被丢弃。
var ErrChannelFull = errors.New("eventbus: subscriber channel full")
