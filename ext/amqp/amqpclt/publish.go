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

// Publish 实现 AMQP 发布者，内置 Publisher Confirms 和指数退避重试。
type Publish struct {
	ch           *amqp.Channel
	exchange     string
	log          func(context.Context, ...any)
	maxRetries   int
	retryBackoff time.Duration
}

// PublishOption 发布者配置项。
type PublishOption func(*Publish)

// WithMaxRetries 设置发布失败时的最大重试次数，默认 3。
func WithMaxRetries(n int) PublishOption {
	return func(p *Publish) { p.maxRetries = n }
}

// WithRetryBackoff 设置重试基础退避间隔，默认 100ms，指数增长。
func WithRetryBackoff(d time.Duration) PublishOption {
	return func(p *Publish) { p.retryBackoff = d }
}

// NewPublisher 创建 AMQP 发布者并启用 Publisher Confirms 模式。
func NewPublisher(conn *amqp.Connection, exchange string, opts ...PublishOption) (*Publish, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("cannot allocate channel: %v", err)
	}
	if err = declareExchange(ch, exchange); err != nil {
		ch.Close()
		return nil, fmt.Errorf("cannot declare exchange: %v", err)
	}
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, fmt.Errorf("cannot enable confirm mode: %v", err)
	}
	p := &Publish{
		ch:           ch,
		exchange:     exchange,
		maxRetries:   3,
		retryBackoff: 100 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

// Publish 发布消息到 Broker，等待 Broker 确认（Publisher Confirm）。
// 失败时指数退避重试 maxRetries 次。
func (p *Publish) Publish(ctx context.Context, routingKey, correlationId string, m Message) error {
	if m.PayloadType == "" {
		m.PayloadType = reflect.TypeOf(m.Payload).String()
	}

	body, err := sonic.Marshal(&m)
	if err != nil {
		return fmt.Errorf("cannot marshal: %v", err)
	}

	msg := amqp.Publishing{
		ContentType:   "application/json",
		Headers:       amqp.Table{},
		CorrelationId: correlationId,
		Body:          body,
		DeliveryMode:  amqp.Persistent,
		Timestamp:     time.Now(),
	}

	return p.publishWithRetry(ctx, routingKey, msg)
}

// publishWithRetry 尝试发布并等待 Broker 确认，指数退避重试。
func (p *Publish) publishWithRetry(ctx context.Context, routingKey string, msg amqp.Publishing) error {
	var lastErr error
	for attempt := range p.maxRetries + 1 {
		dc, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, p.exchange, routingKey, false, false, msg)
		if err != nil {
			lastErr = fmt.Errorf("publish (attempt %d/%d): %w", attempt+1, p.maxRetries+1, err)
			if attempt < p.maxRetries {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(p.backoff(attempt)):
				}
			}
			continue
		}

		acked, err := dc.WaitContext(ctx)
		if err != nil {
			return err
		}
		if acked {
			return nil
		}

		lastErr = fmt.Errorf("publish nacked by broker (attempt %d/%d)", attempt+1, p.maxRetries+1)
		if attempt < p.maxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.backoff(attempt)):
			}
		}
	}
	return fmt.Errorf("publish failed after %d retries: %w", p.maxRetries+1, lastErr)
}

func (p *Publish) backoff(attempt int) time.Duration {
	if attempt > 10 {
		attempt = 10
	}
	return p.retryBackoff * (1 << attempt)
}

// Close 关闭底层 AMQP 通道。
func (p *Publish) Close() {
	if p == nil || p.ch == nil {
		return
	}
	_ = p.ch.Close()
}

// ── Batch ────────────────────────────────────────────────────────────────────

// BatchMessage 包含批量发布所需信息。
type BatchMessage struct {
	RoutingKey    string
	CorrelationId string
	Message       Message
}

// PublishBatch 逐条发布并等待 Broker 确认，返回确认失败的消息索引。
func (p *Publish) PublishBatch(ctx context.Context, messages []BatchMessage) ([]int, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	var failedIndices []int
	for i, bm := range messages {
		if err := p.Publish(ctx, bm.RoutingKey, bm.CorrelationId, bm.Message); err != nil {
			p.log(ctx, fmt.Sprintf("batch message %d: %v", i, err))
			failedIndices = append(failedIndices, i)
		}
	}
	return failedIndices, nil
}

// ── Async ────────────────────────────────────────────────────────────────────

// PublishResult 异步发布结果。
type PublishResult struct {
	Index int
	Error error
}

// PublishAsync 异步发布，每条消息独立等待 Broker 确认，结果写入 resultCh。
func (p *Publish) PublishAsync(ctx context.Context, messages []BatchMessage, resultCh chan PublishResult) {
	if len(messages) == 0 {
		close(resultCh)
		return
	}

	go func() {
		defer close(resultCh)
		for i, bm := range messages {
			select {
			case <-ctx.Done():
				resultCh <- PublishResult{Index: i, Error: ctx.Err()}
				return
			default:
			}

			err := p.Publish(ctx, bm.RoutingKey, bm.CorrelationId, bm.Message)
			if err != nil {
				p.log(ctx, fmt.Sprintf("async message %d: %v", i, err))
			}
			resultCh <- PublishResult{Index: i, Error: err}
		}
	}()
}
