package amqpclt

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher 定义发布接口。
type Publisher interface {
	// Publish 发送单条
	Publish(c context.Context, routingKey, correlationId string, m Message) (err error)
	// PublishBatch 批量发送，返回失败的索引列表
	PublishBatch(ctx context.Context, messages []BatchMessage) ([]int, error)
	// PublishAsync 异步发送到结果通道
	PublishAsync(ctx context.Context, messages []BatchMessage, resultCh chan PublishResult)
	// Close 关闭发布者并释放资源
	Close()
}

// Subscriber 定义订阅接口。
type Subscriber interface {
	SubscribeRaw(c context.Context) (<-chan amqp.Delivery, func(), error)
	Subscribe(c context.Context) (chan *Message, func(), error)
}
type Connection struct {
	Conn   *amqp.Connection
	ChFunc func() error
}

type Message struct {
	CorrelationId string    `json:"correlationId"`
	Event         string    `json:"event"`
	PayloadType   string    `json:"payloadType"`
	Payload       any       `json:"payload"`
	Timestamp     time.Time `json:"timestamp"`
}
