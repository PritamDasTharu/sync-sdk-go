package outbox_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/biticonic/sync-sdk-go/event"
	"github.com/biticonic/sync-sdk-go/outbox"
)

type MockRepository struct {
	mu    sync.Mutex
	items map[string]outbox.OutboxItem
}

func NewMockRepository() *MockRepository {
	return &MockRepository{
		items: make(map[string]outbox.OutboxItem),
	}
}

func (m *MockRepository) Add(item outbox.OutboxItem) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[item.ID] = item
}

func (m *MockRepository) FetchPending(ctx context.Context, limit int) ([]outbox.OutboxItem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []outbox.OutboxItem
	for _, item := range m.items {
		if item.Status == outbox.StatusPending {
			result = append(result, item)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *MockRepository) MarkPublished(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if item, exists := m.items[id]; exists {
		item.Status = outbox.StatusPublished
		item.UpdatedAt = time.Now().UTC()
		m.items[id] = item
		return nil
	}
	return outbox.ErrItemNotFound
}

func (m *MockRepository) MarkFailed(ctx context.Context, id string, errStr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if item, exists := m.items[id]; exists {
		item.Status = outbox.StatusFailed
		item.RetryCount++
		item.LastError = errStr
		item.UpdatedAt = time.Now().UTC()
		m.items[id] = item
		return nil
	}
	return outbox.ErrItemNotFound
}

func TestOutboxItemCreation(t *testing.T) {
	payload := []byte(`{"order_id": "ORD-101"}`)
	env := event.NewEnvelope("ztt", "pos_01", "order", "ORD-101", "created", payload)

	item := outbox.NewOutboxItem(env)
	if item.ID != env.ID {
		t.Fatalf("expected item ID %s, got %s", env.ID, item.ID)
	}
	if item.Status != outbox.StatusPending {
		t.Fatalf("expected status PENDING, got %s", item.Status)
	}
}

func TestMockRepositoryStateTransitions(t *testing.T) {
	repo := NewMockRepository()
	ctx := context.Background()

	env := event.NewEnvelope("ztt", "pos_01", "order", "ORD-101", "created", []byte(`{}`))
	item := outbox.NewOutboxItem(env)

	repo.Add(item)

	pending, err := repo.FetchPending(ctx, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("expected 1 pending item, got %d (err: %v)", len(pending), err)
	}

	if err := repo.MarkPublished(ctx, item.ID); err != nil {
		t.Fatalf("MarkPublished failed: %v", err)
	}

	pendingAfter, _ := repo.FetchPending(ctx, 10)
	if len(pendingAfter) != 0 {
		t.Fatalf("expected 0 pending items after publish, got %d", len(pendingAfter))
	}
}

func TestCalculateBackoff(t *testing.T) {
	base := 100 * time.Millisecond
	for attempt := 0; attempt < 5; attempt++ {
		backoff := outbox.CalculateBackoff(attempt, base)
		if backoff < base {
			t.Fatalf("backoff %v should be at least base %v", backoff, base)
		}
	}
}

func TestPublisherConfigDefaults(t *testing.T) {
	repo := NewMockRepository()
	p := outbox.NewPublisher(repo, nil, outbox.PublisherConfig{})

	cfg := p.Config()
	if cfg.PublishTimeout != 10*time.Second {
		t.Fatalf("expected default PublishTimeout of 10s, got %v", cfg.PublishTimeout)
	}
	if cfg.BatchSize != 50 {
		t.Fatalf("expected default BatchSize of 50, got %d", cfg.BatchSize)
	}
	if cfg.PollInterval != 500*time.Millisecond {
		t.Fatalf("expected default PollInterval of 500ms, got %v", cfg.PollInterval)
	}
}

func TestPublisherConfigCustom(t *testing.T) {
	repo := NewMockRepository()
	customCfg := outbox.PublisherConfig{
		PublishTimeout: 5 * time.Second,
		BatchSize:      100,
	}
	p := outbox.NewPublisher(repo, nil, customCfg)

	cfg := p.Config()
	if cfg.PublishTimeout != 5*time.Second {
		t.Fatalf("expected custom PublishTimeout of 5s, got %v", cfg.PublishTimeout)
	}
	if cfg.BatchSize != 100 {
		t.Fatalf("expected custom BatchSize of 100, got %d", cfg.BatchSize)
	}
}

