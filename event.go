package eventbus

import (
	"time"

	"github.com/google/uuid"
)

// Event 事件结构体
type Event struct {
	Id        string    // 唯一标识，自动生成UUID
	Topic     string    // 事件主题（如"user_registered"）
	Payload   any       // 事件载体（可以是任何类型）
	Source    string    // 事件来源（"service"或"amqp"）
	Version   int64     // 版本号
	Timestamp time.Time // 时间戳
	Priority  int64     // 优先级
}

func NewEvent(topic string, payload any) *Event {
	return &Event{
		Id:        uuid.New().String(),
		Topic:     topic,
		Payload:   payload,
		Source:    "service", // 本地服务发布的事件标记为 "service"
		Version:   0,
		Timestamp: time.Now(),
		Priority:  0,
	}
}
