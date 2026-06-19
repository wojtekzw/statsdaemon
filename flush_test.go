package main

import (
	"bytes"
	"testing"
	"time"
)

// TestFlushHandoffRace mirrors the monitor's swap-and-handoff loop: one goroutine
// folds packets into the current metrics and periodically swaps a fresh one in,
// handing the old snapshot to a worker that drains it like submit() does. Run
// under `go test -race` it verifies the ownership transfer is race-free.
func TestFlushHandoffRace(t *testing.T) {
	// Reset flush-side globals this test's worker mutates.
	lastGaugeValue = make(map[string]float64)
	countInactivity = make(map[string]int64)

	jobs := make(chan flushJob, 8)
	done := make(chan struct{})
	go func() {
		for job := range jobs {
			var buf bytes.Buffer
			now := job.deadline.Unix()
			// reset=true + nil db: persistence path is skipped in reset mode.
			job.m.processCounters(&buf, now, true, "dummy", nil)
			job.m.processGauges(&buf, now, "dummy")
			job.m.processTimers(&buf, now, Percentiles{}, "dummy")
			job.m.processSets(&buf, now, "dummy")
			job.m.processKeyValue(&buf, now, "dummy")
		}
		close(done)
	}()

	cur := newMetrics()
	for i := 0; i < 2000; i++ {
		cur.handlePacket(&Packet{Bucket: "race.counter", Value: int64(1), Modifier: "c", Sampling: 1})
		cur.handlePacket(&Packet{Bucket: "race.timer", Value: float64(i), Modifier: "ms", Sampling: 1})
		cur.handlePacket(&Packet{Bucket: "race.gauge", Value: GaugeData{false, false, float64(i)}, Modifier: "g", Sampling: 1})
		if i%200 == 0 {
			jobs <- flushJob{m: cur, deadline: time.Now()}
			cur = newMetrics() // monitor stops touching the handed-off snapshot
		}
	}
	jobs <- flushJob{m: cur, deadline: time.Now()}
	close(jobs)
	<-done
}
