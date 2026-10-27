package nats

import (
	"fmt"
	"crypto/tls"
	"time"

	"github.com/nats-io/nats.go"
)

// Config defines connection options for NATS transport.
type Config struct {
	URL            string        // e.g. "nats://localhost:4222" or "wss://sync.domain.com"
	UserCredsFile  string        // Path to .creds file for NKEY / JWT auth
	ConnectTimeout time.Duration // Connection timeout (default: 10s)
	MaxReconnects  int           // Max reconnection attempts (default: -1 unlimited)
	ReconnectWait  time.Duration // Time to wait between reconnect attempts (default: 2s)
	InsecureSkipVerify bool          // Skip TLS verification (useful for self-signed certificates or dev WSS)
}

// Client manages NATS connection and JetStream context.
type Client struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

// NewClient initializes a NATS connection and JetStream context.
func NewClient(cfg Config) (*Client, error) {
	if cfg.URL == "" {
		cfg.URL = nats.DefaultURL
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.ReconnectWait == 0 {
		cfg.ReconnectWait = 2 * time.Second
	}
	if cfg.MaxReconnects == 0 {
		cfg.MaxReconnects = -1 // Unlimited
	}

	opts := []nats.Option{
		nats.Timeout(cfg.ConnectTimeout),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.PingInterval(20 * time.Second),
		nats.MaxPingsOutstanding(3),
	}

	if cfg.InsecureSkipVerify {
		opts = append(opts, nats.Secure(&tls.Config{InsecureSkipVerify: true}))
	}

	if cfg.UserCredsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.UserCredsFile))
	}

	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats transport: connection failed to %s: %w", cfg.URL, err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats transport: jetstream context initialization failed: %w", err)
	}

	return &Client{
		nc: nc,
		js: js,
	}, nil
}

// Conn returns the raw NATS connection.
func (c *Client) Conn() *nats.Conn {
	return c.nc
}

// JetStream returns the JetStream context.
func (c *Client) JetStream() nats.JetStreamContext {
	return c.js
}

// Close gracefully closes the NATS connection.
func (c *Client) Close() {
	if c.nc != nil {
		c.nc.Close()
	}
}

// BuildSubject produces a canonical subject string: <app>.<source>.<entity>.<action>
func BuildSubject(appID, source, entity, action string) string {
	return fmt.Sprintf("%s.%s.%s.%s", appID, source, entity, action)
}

