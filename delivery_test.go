package eventbus

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

func silenceLogs(t *testing.T) {
	t.Helper()
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
}

func TestPublishReportReceipts(t *testing.T) {
	silenceLogs(t)
	bus := New()
	defer bus.Close()

	ctx := context.Background()
	exactCh := bus.Subscribe(ctx, "order.created")
	wildCh := bus.Subscribe(ctx, "order.*")

	event := NewEvent("order.created", "payload")
	receipts, err := bus.PublishReport(ctx, event)
	if err != nil {
		t.Fatalf("PublishReport: %v", err)
	}
	if len(receipts) != 2 {
		t.Fatalf("receipts = %d, want 2: %+v", len(receipts), receipts)
	}

	seenIDs := map[string]bool{}
	for _, r := range receipts {
		if r.EventID != event.Id {
			t.Errorf("EventID = %q, want %q", r.EventID, event.Id)
		}
		if r.Status != DeliveryQueued {
			t.Errorf("Status = %s, want queued", r.Status)
		}
		if r.SubscriberID == "" {
			t.Error("SubscriberID must not be empty")
		}
		if seenIDs[r.SubscriberID] {
			t.Errorf("duplicate SubscriberID %q", r.SubscriberID)
		}
		seenIDs[r.SubscriberID] = true
	}
	if receipts[0].SubscriptionTopic != "order.created" {
		t.Errorf("exact SubscriptionTopic = %q", receipts[0].SubscriptionTopic)
	}
	if receipts[1].SubscriptionTopic != "order.*" {
		t.Errorf("wildcard SubscriptionTopic = %q", receipts[1].SubscriptionTopic)
	}

	// 两个通道都应该收到事件。
	if e := <-exactCh; e != event {
		t.Error("exact subscriber did not receive event")
	}
	if e := <-wildCh; e != event {
		t.Error("wildcard subscriber did not receive event")
	}
}

func TestPublishReportDropped(t *testing.T) {
	silenceLogs(t)
	bus := New()
	defer bus.Close()

	ctx := context.Background()
	bus.Subscribe(ctx, "full.topic") // 不消费，容量 64

	var last DeliveryReceipt
	for i := 0; i < 65; i++ {
		receipts, err := bus.PublishReport(ctx, NewEvent("full.topic", i))
		if err != nil {
			t.Fatalf("PublishReport: %v", err)
		}
		if len(receipts) != 1 {
			t.Fatalf("receipts = %d, want 1", len(receipts))
		}
		last = receipts[0]
	}
	if last.Status != DeliveryDropped {
		t.Fatalf("65th receipt status = %s, want dropped", last.Status)
	}
}

func TestPublishReportNoSubscriber(t *testing.T) {
	bus := New()
	defer bus.Close()

	receipts, err := bus.PublishReport(context.Background(), NewEvent("none", nil))
	if err != nil {
		t.Fatalf("PublishReport: %v", err)
	}
	if len(receipts) != 0 {
		t.Fatalf("receipts = %d, want 0", len(receipts))
	}
}

func TestPublishReportMiddlewareError(t *testing.T) {
	bus := New()
	defer bus.Close()

	wantErr := errors.New("middleware rejected")
	bus.Use(func(next Handler) Handler {
		return HandlerFunc(func(context.Context, *Event) error { return wantErr })
	})

	receipts, err := bus.PublishReport(context.Background(), NewEvent("blocked", nil))
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if len(receipts) != 0 {
		t.Fatalf("receipts = %d, want 0", len(receipts))
	}
}

func TestPublishReportAfterUnsubscribe(t *testing.T) {
	bus := New()
	defer bus.Close()

	ctx := context.Background()
	sub := bus.SubscribeAsync(ctx, "removed", HandlerFunc(func(context.Context, *Event) error {
		return nil
	}), 1)
	sub.Unsubscribe()

	receipts, err := bus.PublishReport(ctx, NewEvent("removed", nil))
	if err != nil {
		t.Fatalf("PublishReport: %v", err)
	}
	if len(receipts) != 0 {
		t.Fatalf("receipts = %d after Unsubscribe, want 0", len(receipts))
	}
}
