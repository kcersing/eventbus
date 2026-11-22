package amqpclt

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

// 指数退避在第 10 次尝试后封顶。
func TestPublishBackoff(t *testing.T) {
	p := &Publish{retryBackoff: 100 * time.Millisecond}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{10, 102400 * time.Millisecond},
		{11, 102400 * time.Millisecond},
	}
	for _, tc := range tests {
		if got := p.backoff(tc.attempt); got != tc.want {
			t.Fatalf("backoff(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

// Payload 无法序列化时，Publish 应在触达 channel 之前返回错误。
func TestPublishMarshalError(t *testing.T) {
	p := &Publish{}
	err := p.Publish(context.Background(), "rk", "cid", Message{Payload: math.NaN()})
	if err == nil {
		t.Fatal("expected marshal error")
	}
	if !strings.Contains(err.Error(), "cannot marshal") {
		t.Fatalf("error = %v, want contains cannot marshal", err)
	}
}

// 空批次直接返回，不触碰底层 channel。
func TestPublishBatchEmpty(t *testing.T) {
	failed, err := (&Publish{}).PublishBatch(context.Background(), nil)
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if failed != nil {
		t.Fatalf("failed indices = %v, want nil", failed)
	}
}

// 空消息列表应立即关闭结果通道。
func TestPublishAsyncEmptyClosesResult(t *testing.T) {
	ch := make(chan PublishResult)
	(&Publish{}).PublishAsync(context.Background(), nil, ch)
	if _, ok := <-ch; ok {
		t.Fatal("expected closed result channel")
	}
}

// ctx 已取消时，PublishAsync 应为待发送消息返回 ctx 错误并关闭通道。
func TestPublishAsyncCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := make(chan PublishResult, 1)
	(&Publish{}).PublishAsync(ctx, []BatchMessage{{RoutingKey: "rk"}}, ch)

	res, ok := <-ch
	if !ok {
		t.Fatal("expected one result")
	}
	if res.Index != 0 {
		t.Fatalf("result index = %d, want 0", res.Index)
	}
	if !errors.Is(res.Error, context.Canceled) {
		t.Fatalf("result error = %v, want context.Canceled", res.Error)
	}
	if _, ok := <-ch; ok {
		t.Fatal("expected closed result channel")
	}
}

// 发布者选项应写入对应字段。
func TestPublishOptions(t *testing.T) {
	p := &Publish{}
	WithMaxRetries(5)(p)
	WithRetryBackoff(2 * time.Second)(p)

	if p.maxRetries != 5 {
		t.Fatalf("maxRetries = %d, want 5", p.maxRetries)
	}
	if p.retryBackoff != 2*time.Second {
		t.Fatalf("retryBackoff = %v, want 2s", p.retryBackoff)
	}
}
