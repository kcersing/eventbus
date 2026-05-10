package amqpclt

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/bytedance/sonic"
	amqp "github.com/rabbitmq/amqp091-go"
)

var _ Publisher = (*Publish)(nil)

// Publisher implements an amqp publisher.
type Publish struct {
	ch       *amqp.Channel // AMQP 通道
	exchange string        // 交换机名称
	log      func(context.Context, ...any)
}

func NewPublisher(conn *amqp.Connection, exchange string) (*Publish, error) {
	// 创建通道
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("cannot allocate channel: %v", err)
	}
	// 声明交换机
	if err = declareExchange(ch, exchange); err != nil {
		return nil, fmt.Errorf("cannot declare exchange: %v", err)
	}
	return &Publish{
		ch:       ch,
		exchange: exchange,
	}, nil
}

// Publish publishes a message.
func (p *Publish) Publish(ctx context.Context, routingKey, correlationId string, m Message) error {
	// 将消息结构体序列化为 JSON

	if m.PayloadType == "" {
		m.PayloadType = reflect.TypeOf(m.Payload).String()
	}

	body, err := sonic.Marshal(&m)

	if err != nil {
		return fmt.Errorf("cannot marshal: %v", err)
	}

	headers := amqp.Table{}

	msg := amqp.Publishing{
		ContentType:   "application/json",
		Headers:       headers,
		CorrelationId: correlationId,
		Body:          body,
		DeliveryMode:  amqp.Persistent, // 消息持久化
		Timestamp:     time.Now(),
	}

	return p.ch.PublishWithContext(
		ctx,
		p.exchange,
		routingKey,
		false,
		false,
		msg,
	)
}

// Close closes the underlying channel of the publisher.
func (p *Publish) Close() {
	if p == nil || p.ch == nil {
		return
	}
	_ = p.ch.Close()
}

// BatchMessage 包含批量发布所需的信息
type BatchMessage struct {
	RoutingKey    string
	CorrelationId string
	Message       Message
}

// PublishBatch 批量发布消息，返回失败的消息索引列表
func (p *Publish) PublishBatch(ctx context.Context, messages []BatchMessage) ([]int, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	var failedIndices []int

	for i, bm := range messages {
		body, err := sonic.Marshal(&bm.Message)
		if err != nil {
			p.log(ctx, fmt.Sprintf("batch message %d: cannot marshal: %v", i, err))
			failedIndices = append(failedIndices, i)
			continue
		}

		msg := amqp.Publishing{
			ContentType:   "application/json",
			Headers:       amqp.Table{},
			CorrelationId: bm.CorrelationId,
			Body:          body,
			DeliveryMode:  amqp.Persistent,
			Timestamp:     time.Now(),
		}

		if err := p.ch.PublishWithContext(ctx, p.exchange, bm.RoutingKey, false, false, msg); err != nil {
			p.log(ctx, fmt.Sprintf("batch message %d: publish failed: %v", i, err))
			failedIndices = append(failedIndices, i)
		}
	}

	return failedIndices, nil
}

// PublishResult 表示异步发布的结果
type PublishResult struct {
	Index int
	Error error
}

// PublishAsync 异步发布消息到通道，调用者应通过结果通道获取各条消息的发布结果
func (p *Publish) PublishAsync(ctx context.Context, messages []BatchMessage, resultCh chan PublishResult) {
	if len(messages) == 0 {
		close(resultCh)
		return
	}

	go func() {
		defer close(resultCh) // 确保通道被关闭（避免死锁）
		for i, bm := range messages {
			select {
			case <-ctx.Done(): // 检查上下文是否取消（超时/手动取消）
				resultCh <- PublishResult{Index: i, Error: ctx.Err()}
				return // 上下文取消后立即返回
			default:
			}

			body, err := sonic.Marshal(&bm.Message)
			if err != nil {
				p.log(ctx, fmt.Sprintf("async message %d: cannot marshal: %v", i, err))
				resultCh <- PublishResult{Index: i, Error: fmt.Errorf("marshal error: %w", err)}
				continue
			}

			msg := amqp.Publishing{
				ContentType:   "application/json",
				Headers:       amqp.Table{},
				CorrelationId: bm.CorrelationId,
				Body:          body,
				DeliveryMode:  amqp.Persistent,
				Timestamp:     time.Now(),
			}

			if err := p.ch.PublishWithContext(ctx, p.exchange, bm.RoutingKey, false, false, msg); err != nil {
				p.log(ctx, fmt.Sprintf("async message %d: publish failed: %v", i, err))
				resultCh <- PublishResult{Index: i, Error: fmt.Errorf("publish error: %w", err)}
			} else {
				resultCh <- PublishResult{Index: i, Error: nil}
			}
		}
	}()
}
