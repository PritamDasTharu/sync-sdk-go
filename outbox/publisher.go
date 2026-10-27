package outbox

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/biticonic/sync-sdk-go/event"
	natsTransport "github.com/biticonic/sync-sdk-go/transport/nats"
	"github.com/nats-io/nats.go"
)

// PublisherConfig holds settings for the background Outbox publisher worker.
type PublisherConfig struct {
	BatchSize      int           // Number of pending items to poll per cycle (default 50)
	PollInterval   time.Duration // Time between poll cycles (default 500ms)
	MaxRetries     int           // Max retries before marking an item as FAILED (default 5)
	BaseBackoff    time.Duration // Initial retry delay (default 100ms)
	PublishTimeout time.Duration // Timeout for publishing an individual item (default 10s)
}

// Publisher is a background worker that polls the local Outbox repository and publishes events to NATS JetStream.
type Publisher struct {
	repo       Repository
	natsClient *natsTransport.Client
	serializer *event.Serializer
	cfg        PublisherConfig
	mu         sync.Mutex
	running    bool
	stopChan   chan struct{}
}

// NewPublisher initializes a new Outbox Publisher worker.
func NewPublisher(repo Repository, natsClient *natsTransport.Client, cfg PublisherConfig) *Publisher {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 100 * time.Millisecond
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = 10 * time.Second
	}

	return &Publisher{
		repo:       repo,
		natsClient: natsClient,
		serializer: event.NewSerializer(),
		cfg:        cfg,
		stopChan:   make(chan struct{}),
	}
}

// Config returns the active PublisherConfig settings.
func (p *Publisher) Config() PublisherConfig {
	return p.cfg
}

// Start launches the background polling loop.
func (p *Publisher) Start(ctx context.Context) {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return
	}
	p.running = true
	p.mu.Unlock()

	go p.loop(ctx)
}

// Stop gracefully signals the worker loop to stop.
func (p *Publisher) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return
	}
	p.running = false
	close(p.stopChan)
}

func (p *Publisher) loop(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopChan:
			return
		case <-ticker.C:
			p.processBatch(ctx)
		}
	}
}

// ProcessBatch processes a single batch of pending outbox events (exposed for testing & manual triggers).
func (p *Publisher) ProcessBatch(ctx context.Context) int {
	return p.processBatch(ctx)
}

func (p *Publisher) processBatch(ctx context.Context) int {
	items, err := p.repo.FetchPending(ctx, p.cfg.BatchSize)
	if err != nil || len(items) == 0 {
		return 0
	}

	processedCount := 0
	for _, item := range items {
		if ctx.Err() != nil {
			break
		}

		if err := p.publishItem(ctx, item); err != nil {
			errStr := fmt.Sprintf("publish error: %v", err)
			_ = p.repo.MarkFailed(ctx, item.ID, errStr)
			// Break batch on network/publish failure to prevent exhausting retries on all items simultaneously
			break
		} else {
			_ = p.repo.MarkPublished(ctx, item.ID)
			processedCount++
		}
	}

	return processedCount
}

func (p *Publisher) publishItem(ctx context.Context, item OutboxItem) error {
	data, err := p.serializer.Marshal(item.Envelope)
	if err != nil {
		return fmt.Errorf("failed to marshal envelope: %w", err)
	}

	subject := item.Envelope.Subject()

	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  make(nats.Header),
	}
	// Attach Nats-Msg-Id for NATS broker-level deduplication window
	msg.Header.Set("Nats-Msg-Id", item.Envelope.ID)

	js := p.natsClient.JetStream()
	if js == nil {
		return fmt.Errorf("jetstream context is nil")
	}

	// Enforce bounded publish timeout to prevent indefinite socket hangs
	pubCtx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
	defer cancel()

	_, err = js.PublishMsg(msg, nats.Context(pubCtx))
	if err != nil {
		return fmt.Errorf("jetstream publish failed on subject %s: %w", subject, err)
	}

	return nil
}

// CalculateBackoff computes exponential backoff duration with random jitter.
func CalculateBackoff(attempt int, base time.Duration) time.Duration {
	if attempt <= 0 {
		return base
	}
	temp := float64(base) * float64(int64(1)<<uint(attempt))
	jitter := temp * 0.2 * rand.Float64()
	return time.Duration(temp + jitter)
}

