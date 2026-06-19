package main

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestAtomicMax(t *testing.T) {
	var x int64
	atomicMax(&x, 5)
	if x != 5 {
		t.Fatalf("after max(0,5) = %d, want 5", x)
	}
	atomicMax(&x, 3) // smaller, must not change
	if x != 5 {
		t.Fatalf("after max(5,3) = %d, want 5", x)
	}
	atomicMax(&x, 10)
	if x != 10 {
		t.Fatalf("after max(5,10) = %d, want 10", x)
	}
}

func TestAtomicMaxConcurrent(t *testing.T) {
	var x int64
	var wg sync.WaitGroup
	for i := 1; i <= 200; i++ {
		wg.Add(1)
		go func(v int64) {
			defer wg.Done()
			atomicMax(&x, v)
		}(int64(i))
	}
	wg.Wait()
	if x != 200 {
		t.Errorf("concurrent atomicMax = %d, want 200", x)
	}
}

func TestSwapCounter(t *testing.T) {
	var x int64
	atomic.AddInt64(&x, 7)
	if got := swapCounter(&x); got != 7 {
		t.Errorf("swapCounter = %d, want 7", got)
	}
	if x != 0 {
		t.Errorf("swapCounter did not reset, x = %d", x)
	}
	if got := swapCounter(&x); got != 0 {
		t.Errorf("swapCounter on drained counter = %d, want 0", got)
	}
}

// TestDaemonStatSnapshot exercises the lock-free counter increments and the
// atomic read-and-reset performed by ProcessStats.
func TestDaemonStatSnapshot(t *testing.T) {
	ds := &DaemonStat{Interval: 10}

	ds.PointsReceivedInc()
	ds.PointsReceivedInc()
	ds.PointTypeInc(&Packet{Modifier: "c"})
	ds.PointTypeInc(&Packet{Modifier: "g"})
	ds.BytesReceivedInc(50)
	ds.PacketCacheHit()

	ds.ProcessStats(packetCache, nameCache)

	checks := map[string]struct{ got, want int64 }{
		"PointsReceived":        {ds.savedStat.PointsReceived, 2},
		"PointsReceivedCounter": {ds.savedStat.PointsReceivedCounter, 1},
		"PointsReceivedGauge":   {ds.savedStat.PointsReceivedGauge, 1},
		"BytesReceived":         {ds.savedStat.BytesReceived, 50},
		"PacketCacheHit":        {ds.savedStat.PacketCacheHit, 1},
	}
	for name, c := range checks {
		if c.got != c.want {
			t.Errorf("savedStat.%s = %d, want %d", name, c.got, c.want)
		}
	}

	if want := float64(2) / float64(10); ds.savedStat.PointsReceivedRate != want {
		t.Errorf("PointsReceivedRate = %v, want %v", ds.savedStat.PointsReceivedRate, want)
	}

	// Counters must be reset after the snapshot.
	if ds.curStat.PointsReceived != 0 {
		t.Errorf("curStat.PointsReceived not reset: %d", ds.curStat.PointsReceived)
	}

	// A second snapshot with no new data must report zeros.
	ds.ProcessStats(packetCache, nameCache)
	if ds.savedStat.PointsReceived != 0 {
		t.Errorf("second snapshot PointsReceived = %d, want 0", ds.savedStat.PointsReceived)
	}
}
