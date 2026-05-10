package eventbus

import (
	"encoding/binary"
	"encoding/hex"
	"math/rand/v2"
	"time"
)

// Event 是事件总线上传递的基本单元。
type Event struct {
	Id        string    // 全局唯一标识（纳秒时间戳 8B + 随机数 4B，24 字符 hex）
	Topic     string    // 事件主题，如 "order.created"
	Payload   any       // 事件负载，可以是任意类型
	Source    string    // 事件来源，由发布方填写，如 "local"、"amqp"
	Timestamp time.Time // 事件创建时间
}

// NewEvent 创建事件，自动生成唯一 ID 和时间戳。
func NewEvent(topic string, payload any) *Event {
	now := time.Now()
	return &Event{
		Id:        newEventID(now),
		Topic:     topic,
		Payload:   payload,
		Source:    "local",
		Timestamp: now,
	}
}

// newEventID 生成全局唯一 ID：纳秒时间戳（8B）+ 随机数（4B）→ 24 字符 hex。
func newEventID(now time.Time) string {
	var b [12]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(now.UnixNano()))
	binary.BigEndian.PutUint32(b[8:12], rand.Uint32())
	return hex.EncodeToString(b[:])
}
