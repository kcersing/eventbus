package eventbus

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"time"
)

// newEventID 生成全局唯一的事件 ID。
// 格式：12 字节 → 24 字符 hex（纳秒时间戳 8B + 加密随机数 4B），纳秒前缀保证时间有序。
func newEventID() string {
	var b [12]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(time.Now().UnixNano()))
	_, _ = rand.Read(b[8:12])
	return hex.EncodeToString(b[:])
}

// Event 事件结构体，在总线上传递的基本单元。
type Event struct {
	Id        string    // 全局唯一标识（纳秒时间戳+随机数，24 字符 hex）
	Topic     string    // 事件主题，如 "order.created"、"user.registered"
	Payload   any       // 事件负载，可以是任意类型
	Source    string    // 事件来源："Local"（本地）、"Distributed"（本地+MQ）、"amqp"（来自 MQ）
	Version   int64     // 负载版本号，预留用于 schema 演进
	Timestamp time.Time // 事件创建时间
	Priority  int64     // 优先级（预留，暂未使用）
}

// NewEvent 创建事件，自动生成唯一 ID 和时间戳。
func NewEvent(topic string, payload any) *Event {
	return &Event{
		Id:        newEventID(),
		Topic:     topic,
		Payload:   payload,
		Source:    "Local",
		Version:   0,
		Timestamp: time.Now(),
		Priority:  0,
	}
}
