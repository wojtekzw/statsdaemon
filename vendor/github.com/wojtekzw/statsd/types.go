package statsd

import (
	"time"
)

// Statser - interface for StatsD Client
type Statser interface {
	Clone(opts ...Option) Statser
	Count(bucket string, n interface{})
	Increment(bucket string)
	Gauge(bucket string, value interface{})
	Timing(bucket string, value interface{})
	Histogram(bucket string, value interface{})
	Unique(bucket string, value string)
	Flush()
	Close()
	NewTiming() Timinger
}

// A Timinger is an helper object that eases sending timing values.
type Timinger interface {
	Send(bucket string)
	Duration() time.Duration
}
