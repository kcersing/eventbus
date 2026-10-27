package eventbus

import (
	"sync"
	"testing"
	"time"
)

// - TestNewEvent — 验证 ID 格式（24 字符 hex）、Topic、Source、Timestamp
// - TestNewEvent_IDUnique — 100 并发验证 ID 无重复
func TestNewEvent(t *testing.T) {
	payload := map[string]any{"order_id": 42}
	e := NewEvent("order.created", payload)

	if e.Id == "" {
		t.Error("Event.Id must not be empty")
	}
	if len(e.Id) != 24 {
		t.Errorf("Event.Id length = %d, want 24 (12 bytes hex)", len(e.Id))
	}
	if e.Topic != "order.created" {
		t.Errorf("Event.Topic = %q, want %q", e.Topic, "order.created")
	}
	if e.Source != "local" {
		t.Errorf("Event.Source = %q, want %q", e.Source, "local")
	}
	if time.Since(e.Timestamp) > time.Second {
		t.Error("Event.Timestamp should be close to now")
	}
}

func TestNewEvent_IDUnique(t *testing.T) {
	ids := make(map[string]bool)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := NewEvent("test", nil)
			mu.Lock()
			if ids[e.Id] {
				t.Errorf("duplicate ID: %s", e.Id)
			}
			ids[e.Id] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
}
