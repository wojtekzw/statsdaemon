package statsd

import (
	"time"
)

// A NoopClient represents a null StatsD client.
type NoopClient struct{}

// NoopTiming - represents null timing
type NoopTiming struct{}

// Clone returns a new NoopClient.
func (c *NoopClient) Clone(opts ...Option) Statser {
	return &NoopClient{}
}

// Count does nothing
func (c *NoopClient) Count(bucket string, n interface{}) {
}

// Increment does nothing
func (c *NoopClient) Increment(bucket string) {
}

// Gauge does nothing
func (c *NoopClient) Gauge(bucket string, value interface{}) {
}

// Timing does nothing
func (c *NoopClient) Timing(bucket string, value interface{}) {
}

// Histogram does nothing
func (c *NoopClient) Histogram(bucket string, value interface{}) {
}

// NewTiming creates a new Timing.
func (c *NoopClient) NewTiming() Timinger {
	return NoopTiming{}
}

// Unique does nothing
func (c *NoopClient) Unique(bucket string, value string) {
}

// Flush does nothing
func (c *NoopClient) Flush() {
}

// Close does nothing
func (c *NoopClient) Close() {
}

// Send does nothing
func (t NoopTiming) Send(bucket string) {
}

// Duration does nothing
func (t NoopTiming) Duration() time.Duration {
	return 0
}
