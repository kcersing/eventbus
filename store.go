package eventbus

import (
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
)

// PersistentEvent 用于储存的事件类型
type PersistentEvent struct {
	Id          int64
	Topic       string // 修复：首字母大写
	Payload     []byte
	IsProcessed bool
	CreatedAt   time.Time
}

// EventStore 储存接口 - 用于事件持久化
type EventStore interface {
	// SaveEvent 保存事件到存储
	SaveEvent(event PersistentEvent) error
	// GetUnprocessedEvents 获取所有未处理的事件
	GetUnprocessedEvents() ([]PersistentEvent, error)
	// MarkEventAsProcessed 标记事件已处理
	MarkEventAsProcessed(eventId int64) error
	// DeleteEvent 删除已处理的事件（可选清理）
	DeleteEvent(eventId int64) error
}

// ============ 简单的内存实现 ============

// InMemoryEventStore 基于内存的事件存储实现
type InMemoryEventStore struct {
	mu     sync.RWMutex
	events map[int64]PersistentEvent
	nextId int64
}

// NewInMemoryEventStore 创建内存事件存储
func NewInMemoryEventStore() *InMemoryEventStore {
	return &InMemoryEventStore{
		events: make(map[int64]PersistentEvent),
		nextId: 1,
	}
}

// SaveEvent 保存事件到内存存储
func (store *InMemoryEventStore) SaveEvent(event PersistentEvent) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if event.Id == 0 {
		event.Id = store.nextId
		store.nextId++
	}

	store.events[event.Id] = event
	klog.Infof("[EventStore] Event saved: id=%d, topic=%s", event.Id, event.Topic)
	return nil
}

// GetUnprocessedEvents 获取所有未处理事件
func (store *InMemoryEventStore) GetUnprocessedEvents() ([]PersistentEvent, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	var unprocessed []PersistentEvent
	for _, event := range store.events {
		if !event.IsProcessed {
			unprocessed = append(unprocessed, event)
		}
	}
	return unprocessed, nil
}

// MarkEventAsProcessed 标记事件已处理
func (store *InMemoryEventStore) MarkEventAsProcessed(eventId int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if event, exists := store.events[eventId]; exists {
		event.IsProcessed = true
		store.events[eventId] = event
		klog.Infof("[EventStore] Event marked as processed: id=%d", eventId)
		return nil
	}
	return nil // 不存在也返回nil（幂等）
}

// DeleteEvent 删除已处理的事件
func (store *InMemoryEventStore) DeleteEvent(eventId int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, exists := store.events[eventId]; exists {
		delete(store.events, eventId)
		klog.Infof("[EventStore] Event deleted: id=%d", eventId)
		return nil
	}
	return nil
}
