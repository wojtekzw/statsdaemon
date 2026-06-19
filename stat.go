package main

import (
	"fmt"
	"github.com/patrickmn/go-cache"
	"runtime"
	"sync/atomic"
	"time"
)

type internalDaemonStat struct {
	PointsReceived         int64
	PointsReceivedRate     float64
	PointsParseFail        int64
	PointsSoftParseFail    int64
	BytesReceived          int64
	ReadFail               int64
	BatchesTransmitted     int64
	BatchesTransmitFail    int64
	PointsTransmitted      int64
	OtherErrors            int64
	PointsReceivedCounter  int64
	PointsReceivedGauge    int64
	PointsReceivedSet      int64
	PointsReceivedTimer    int64
	PointsReceivedKeyValue int64
	MemAlloc               uint64
	MemSys                 uint64
	MemHeapInuse           uint64
	QueueLen               int64
	PacketCacheHit         int64
	PacketCacheMiss        int64
	PacketCacheSize        int64
	NameCacheHit           int64
	NameCacheMiss          int64
	NameCacheSize          int64
	Goroutines             int64
}

type DaemonStat struct {
	curStat   internalDaemonStat
	savedStat internalDaemonStat
	Interval  int64
}

// atomicMax - lock-free update of *addr to max(*addr, v).
func atomicMax(addr *int64, v int64) {
	for {
		old := atomic.LoadInt64(addr)
		if v <= old || atomic.CompareAndSwapInt64(addr, old, v) {
			return
		}
	}
}

func (ds *DaemonStat) Init(q chan *Packet, t time.Duration, interval int64) {
	// batch send interval
	ds.Interval = interval

	// start background incoming queue len monitoring every t time
	go ds.QueueStats(q, t)

	// monitor number of goroutines
	go ds.GoroutinesStats(t)

}

// GlobalVarsSizeToString reports map sizes for the metrics snapshot being
// flushed (mx) plus the flush-side state. It must be called from the flush
// goroutine only, since mx and the flush-side maps are owned there.
func (ds *DaemonStat) GlobalVarsSizeToString(mx *metrics) string {
	s := fmt.Sprintf("counters: %d, gauges: %d, lastGaugeValue: %d, timers: %d, countInactivity: %d, sets: %d, keys: %d",
		len(mx.counters), len(mx.gauges), len(lastGaugeValue), len(mx.timers), len(countInactivity), len(mx.sets), len(mx.keys))
	return s
}

func (ds *DaemonStat) String(mx *metrics) string {
	s := fmt.Sprintf("PointsReceived: %d ops, ", ds.savedStat.PointsReceived)
	s = s + fmt.Sprintf("PointsReceivedRate: %.2f ops/s, ", ds.savedStat.PointsReceivedRate)
	s = s + fmt.Sprintf("PointsParseFail: %d ops, ", ds.savedStat.PointsParseFail)
	s = s + fmt.Sprintf("PointsSoftParseFail: %d ops, ", ds.savedStat.PointsSoftParseFail)
	s = s + fmt.Sprintf("BytesReceived: %d bytes, ", ds.savedStat.BytesReceived)
	s = s + fmt.Sprintf("ReadFail: %d ops, ", ds.savedStat.ReadFail)
	s = s + fmt.Sprintf("BatchesTransmitted: %d ops, ", ds.savedStat.BatchesTransmitted)
	s = s + fmt.Sprintf("BatchesTransmitFail: %d ops, ", ds.savedStat.BatchesTransmitFail)
	s = s + fmt.Sprintf("PointsTransmitted: %d ops, ", ds.savedStat.PointsTransmitted)
	s = s + fmt.Sprintf("OtherErrors: %d errors, ", ds.savedStat.OtherErrors)

	s = s + fmt.Sprintf("MemAlloc: %.0f MB, ", float64(ds.savedStat.MemAlloc)/(1024*1024))
	s = s + fmt.Sprintf("MemSys: %.0f MB, ", float64(ds.savedStat.MemSys)/(1024*1024))
	s = s + fmt.Sprintf("MemHeapInuse: %.0f MB, ", float64(ds.savedStat.MemHeapInuse)/(1024*1024))
	s = s + fmt.Sprintf("QueueLen: %d, ", ds.savedStat.QueueLen)
	s = s + ds.GlobalVarsSizeToString(mx)
	return s
}

func (ds *DaemonStat) Print(mx *metrics) {
	fmt.Printf("%s", ds.String(mx))
}

func (ds *DaemonStat) PointsReceivedInc() {
	atomic.AddInt64(&ds.curStat.PointsReceived, 1)
}

func (ds *DaemonStat) OtherErrorsInc() {
	atomic.AddInt64(&ds.curStat.OtherErrors, 1)
}

func (ds *DaemonStat) BytesReceivedInc(n int64) {
	atomic.AddInt64(&ds.curStat.BytesReceived, n)
}

func (ds *DaemonStat) ReadFailInc() {
	atomic.AddInt64(&ds.curStat.ReadFail, 1)
}

func (ds *DaemonStat) PointsParseFailInc() {
	atomic.AddInt64(&ds.curStat.PointsParseFail, 1)
}

func (ds *DaemonStat) PointsParseSoftFailInc() {
	atomic.AddInt64(&ds.curStat.PointsSoftParseFail, 1)
}

func (ds *DaemonStat) BatchesTransmittedInc() {
	atomic.AddInt64(&ds.curStat.BatchesTransmitted, 1)
}

func (ds *DaemonStat) BatchesTransmitFailInc() {
	atomic.AddInt64(&ds.curStat.BatchesTransmitFail, 1)
}

func (ds *DaemonStat) PointsTransmittedInc(n int64) {
	atomic.AddInt64(&ds.curStat.PointsTransmitted, n)
}

func (ds *DaemonStat) PointTypeInc(p *Packet) {
	switch p.Modifier {
	case "c":
		atomic.AddInt64(&ds.curStat.PointsReceivedCounter, 1)
	case "g":
		atomic.AddInt64(&ds.curStat.PointsReceivedGauge, 1)
	case "s":
		atomic.AddInt64(&ds.curStat.PointsReceivedSet, 1)
	case "ms":
		atomic.AddInt64(&ds.curStat.PointsReceivedTimer, 1)
	case "kv":
		atomic.AddInt64(&ds.curStat.PointsReceivedKeyValue, 1)
	}
}

func (ds *DaemonStat) PacketCacheHit() {
	atomic.AddInt64(&ds.curStat.PacketCacheHit, 1)
}

func (ds *DaemonStat) PacketCacheMiss() {
	atomic.AddInt64(&ds.curStat.PacketCacheMiss, 1)
}

func (ds *DaemonStat) PacketCacheSize(c *cache.Cache) {
	atomic.StoreInt64(&ds.curStat.PacketCacheSize, int64(c.ItemCount()))
}

func (ds *DaemonStat) NameCacheHit() {
	atomic.AddInt64(&ds.curStat.NameCacheHit, 1)
}

func (ds *DaemonStat) NameCacheMiss() {
	atomic.AddInt64(&ds.curStat.NameCacheMiss, 1)
}

func (ds *DaemonStat) NameCacheSize(c *cache.Cache) {
	atomic.StoreInt64(&ds.curStat.NameCacheSize, int64(c.ItemCount()))
}

func (ds *DaemonStat) WriteMetrics(countersMap map[string]int64, gaugesMap map[string]float64, timersMap map[string]Float64Slice, globalPrefix string, metricNamePrefix string, extraTagsStr string) error {

	var ok bool

	if len(metricNamePrefix) == 0 {
		return fmt.Errorf("Empty metric name prefix. No saving to backend")
	}

	versionTag := "statsdaemon=" + StatsdaemonVersion
	// Counters
	versionCounter := makeBucketName(globalPrefix, metricNamePrefix, "version.counter", extraTagsStr, versionTag)
	countersMap[versionCounter] = 1

	pointsReceived := makeBucketName(globalPrefix, metricNamePrefix, "point.received", extraTagsStr, versionTag)
	_, ok = countersMap[pointsReceived]
	if !ok {
		countersMap[pointsReceived] = 0
	}
	countersMap[pointsReceived] += ds.savedStat.PointsReceived

	pointsParseFail := makeBucketName(globalPrefix, metricNamePrefix, "point.parsefail", extraTagsStr, versionTag)
	_, ok = countersMap[pointsParseFail]
	if !ok {
		countersMap[pointsParseFail] = 0
	}
	countersMap[pointsParseFail] += ds.savedStat.PointsParseFail

	pointsSoftParseFail := makeBucketName(globalPrefix, metricNamePrefix, "point.softparsefail", extraTagsStr, versionTag)
	_, ok = countersMap[pointsSoftParseFail]
	if !ok {
		countersMap[pointsSoftParseFail] = 0
	}
	countersMap[pointsSoftParseFail] += ds.savedStat.PointsSoftParseFail

	bytesReceived := makeBucketName(globalPrefix, metricNamePrefix, "read.bytes", extraTagsStr, versionTag)
	_, ok = countersMap[bytesReceived]
	if !ok {
		countersMap[bytesReceived] = 0
	}
	countersMap[bytesReceived] += ds.savedStat.BytesReceived

	readFail := makeBucketName(globalPrefix, metricNamePrefix, "read.fail", extraTagsStr, versionTag)
	_, ok = countersMap[readFail]
	if !ok {
		countersMap[readFail] = 0
	}
	countersMap[readFail] += ds.savedStat.ReadFail

	batchesTransmitted := makeBucketName(globalPrefix, metricNamePrefix, "batch.transmitted", extraTagsStr, versionTag)
	_, ok = countersMap[batchesTransmitted]
	if !ok {
		countersMap[batchesTransmitted] = 0
	}
	countersMap[batchesTransmitted] += ds.savedStat.BatchesTransmitted

	batchesTransmitFail := makeBucketName(globalPrefix, metricNamePrefix, "batch.transmitfail", extraTagsStr, versionTag)
	_, ok = countersMap[batchesTransmitFail]
	if !ok {
		countersMap[batchesTransmitFail] = 0
	}
	countersMap[batchesTransmitFail] += ds.savedStat.BatchesTransmitFail

	pointsTransmitted := makeBucketName(globalPrefix, metricNamePrefix, "point.transmitted", extraTagsStr, versionTag)
	_, ok = countersMap[pointsTransmitted]
	if !ok {
		countersMap[pointsTransmitted] = 0
	}
	countersMap[pointsTransmitted] += ds.savedStat.PointsTransmitted

	otherErrors := makeBucketName(globalPrefix, metricNamePrefix, "error.other", extraTagsStr, versionTag)
	_, ok = countersMap[otherErrors]
	if !ok {
		countersMap[otherErrors] = 0
	}
	countersMap[otherErrors] += ds.savedStat.OtherErrors

	pointsReceivedCounter := makeBucketName(globalPrefix, metricNamePrefix, "point.received.counter", extraTagsStr, versionTag)
	_, ok = countersMap[pointsReceivedCounter]
	if !ok {
		countersMap[pointsReceivedCounter] = 0
	}
	countersMap[pointsReceivedCounter] += ds.savedStat.PointsReceivedCounter

	pointsReceivedGauge := makeBucketName(globalPrefix, metricNamePrefix, "point.received.gauge", extraTagsStr, versionTag)
	_, ok = countersMap[pointsReceivedGauge]
	if !ok {
		countersMap[pointsReceivedGauge] = 0
	}
	countersMap[pointsReceivedGauge] += ds.savedStat.PointsReceivedGauge

	pointsReceivedSet := makeBucketName(globalPrefix, metricNamePrefix, "point.received.set", extraTagsStr, versionTag)
	_, ok = countersMap[pointsReceivedSet]
	if !ok {
		countersMap[pointsReceivedSet] = 0
	}
	countersMap[pointsReceivedSet] += ds.savedStat.PointsReceivedSet

	pointsReceivedTimer := makeBucketName(globalPrefix, metricNamePrefix, "point.received.timer", extraTagsStr, versionTag)
	_, ok = countersMap[pointsReceivedTimer]
	if !ok {
		countersMap[pointsReceivedTimer] = 0
	}
	countersMap[pointsReceivedTimer] += ds.savedStat.PointsReceivedTimer

	pointsReceivedKeyValue := makeBucketName(globalPrefix, metricNamePrefix, "point.received.keyvalue", extraTagsStr, versionTag)
	_, ok = countersMap[pointsReceivedKeyValue]
	if !ok {
		countersMap[pointsReceivedKeyValue] = 0
	}
	countersMap[pointsReceivedKeyValue] += ds.savedStat.PointsReceivedKeyValue

	packetCacheHit := makeBucketName(globalPrefix, metricNamePrefix, "cache.packet.hit", extraTagsStr, versionTag)
	_, ok = countersMap[packetCacheHit]
	if !ok {
		countersMap[packetCacheHit] = 0
	}
	countersMap[packetCacheHit] += ds.savedStat.PacketCacheHit

	packetCacheMiss := makeBucketName(globalPrefix, metricNamePrefix, "cache.packet.miss", extraTagsStr, versionTag)
	_, ok = countersMap[packetCacheMiss]
	if !ok {
		countersMap[packetCacheMiss] = 0
	}
	countersMap[packetCacheMiss] += ds.savedStat.PacketCacheMiss

	nameCacheHit := makeBucketName(globalPrefix, metricNamePrefix, "cache.name.hit", extraTagsStr, versionTag)
	_, ok = countersMap[nameCacheHit]
	if !ok {
		countersMap[nameCacheHit] = 0
	}
	countersMap[nameCacheHit] += ds.savedStat.NameCacheHit

	nameCacheMiss := makeBucketName(globalPrefix, metricNamePrefix, "cache.name.miss", extraTagsStr, versionTag)
	_, ok = countersMap[nameCacheMiss]
	if !ok {
		countersMap[nameCacheMiss] = 0
	}
	countersMap[nameCacheMiss] += ds.savedStat.NameCacheMiss

	// Gauges
	pointsRate := makeBucketName(globalPrefix, metricNamePrefix, "point.received.rate", extraTagsStr, versionTag)
	gaugesMap[pointsRate] = ds.savedStat.PointsReceivedRate

	queueLen := makeBucketName(globalPrefix, metricNamePrefix, "queue.len", extraTagsStr, versionTag)
	gaugesMap[queueLen] = float64(ds.savedStat.QueueLen)

	packetCacheSize := makeBucketName(globalPrefix, metricNamePrefix, "cache.packet.size", extraTagsStr, versionTag)
	gaugesMap[packetCacheSize] = float64(ds.savedStat.PacketCacheSize)

	nameCacheSize := makeBucketName(globalPrefix, metricNamePrefix, "cache.name.size", extraTagsStr, versionTag)
	gaugesMap[nameCacheSize] = float64(ds.savedStat.NameCacheSize)

	goroutines := makeBucketName(globalPrefix, metricNamePrefix, "goroutines.number", extraTagsStr, versionTag)
	gaugesMap[goroutines] = float64(ds.savedStat.Goroutines)

	memAlloc := makeBucketName(globalPrefix, metricNamePrefix, "memory.alloc", extraTagsStr, versionTag)
	gaugesMap[memAlloc] = float64(ds.savedStat.MemAlloc)

	memSys := makeBucketName(globalPrefix, metricNamePrefix, "memory.sys", extraTagsStr, versionTag)
	gaugesMap[memSys] = float64(ds.savedStat.MemSys)

	memHeapInuse := makeBucketName(globalPrefix, metricNamePrefix, "memory.heapinuse", extraTagsStr, versionTag)
	gaugesMap[memHeapInuse] = float64(ds.savedStat.MemHeapInuse)

	return nil

}

// QueueStats - incoming queue len monitoring every t time
func (ds *DaemonStat) QueueStats(c chan *Packet, t time.Duration) {

	for {
		atomicMax(&ds.curStat.QueueLen, int64(len(c)))
		time.Sleep(t)
	}

}

// GoroutinesStats - max number of goroutines monitored  every t time
func (ds *DaemonStat) GoroutinesStats(t time.Duration) {

	for {
		atomicMax(&ds.curStat.Goroutines, int64(runtime.NumGoroutine()))
		time.Sleep(t)
	}

}

// swapCounter - atomically reads a per-interval counter and resets it to 0.
func swapCounter(addr *int64) int64 {
	return atomic.SwapInt64(addr, 0)
}

func (ds *DaemonStat) ProcessStats(packetCache, nameCache *cache.Cache) {

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Atomically snapshot and reset all per-interval counters. Each counter is
	// updated lock-free from the listener/monitor goroutines, so we read-and-reset
	// them with atomic swaps instead of guarding the whole struct with a mutex.
	cur := &ds.curStat
	saved := &ds.savedStat

	saved.PointsReceived = swapCounter(&cur.PointsReceived)
	saved.PointsParseFail = swapCounter(&cur.PointsParseFail)
	saved.PointsSoftParseFail = swapCounter(&cur.PointsSoftParseFail)
	saved.BytesReceived = swapCounter(&cur.BytesReceived)
	saved.ReadFail = swapCounter(&cur.ReadFail)
	saved.BatchesTransmitted = swapCounter(&cur.BatchesTransmitted)
	saved.BatchesTransmitFail = swapCounter(&cur.BatchesTransmitFail)
	saved.PointsTransmitted = swapCounter(&cur.PointsTransmitted)
	saved.OtherErrors = swapCounter(&cur.OtherErrors)
	saved.PointsReceivedCounter = swapCounter(&cur.PointsReceivedCounter)
	saved.PointsReceivedGauge = swapCounter(&cur.PointsReceivedGauge)
	saved.PointsReceivedSet = swapCounter(&cur.PointsReceivedSet)
	saved.PointsReceivedTimer = swapCounter(&cur.PointsReceivedTimer)
	saved.PointsReceivedKeyValue = swapCounter(&cur.PointsReceivedKeyValue)
	saved.PacketCacheHit = swapCounter(&cur.PacketCacheHit)
	saved.PacketCacheMiss = swapCounter(&cur.PacketCacheMiss)
	saved.NameCacheHit = swapCounter(&cur.NameCacheHit)
	saved.NameCacheMiss = swapCounter(&cur.NameCacheMiss)
	saved.QueueLen = swapCounter(&cur.QueueLen)
	saved.Goroutines = swapCounter(&cur.Goroutines)

	// Gauges - sampled here, in this goroutine only.
	saved.MemAlloc = memStats.Alloc
	saved.MemSys = memStats.Sys
	saved.MemHeapInuse = memStats.HeapInuse
	saved.NameCacheSize = int64(nameCache.ItemCount())
	saved.PacketCacheSize = int64(packetCache.ItemCount())
	saved.PointsReceivedRate = float64(saved.PointsReceived) / float64(ds.Interval)

}

func makeBucketName(globalPrefix string, metricNamePrefix string, metricName string, extraTagsStr string, addTagsStr string) string {

	if len(globalPrefix) == 0 && len(metricNamePrefix) == 0 && len(extraTagsStr) == 0 && len(addTagsStr) == 0 {
		return metricName
	}

	sep1 := ""
	sep2 := ""

	if len(extraTagsStr) > 0 {
		sep1 = "^"
	}

	if len(addTagsStr) > 0 {
		sep2 = "^"
	}

	return normalizeDot(globalPrefix, true) + normalizeDot(metricNamePrefix, true) + normalizeDot(metricName, len(extraTagsStr) > 0 || len(addTagsStr) > 0) +
		sep1 + normalizeDot(extraTagsStr, len(addTagsStr) > 0) + sep2 + normalizeDot(addTagsStr, false)
}

func normalizeDot(s string, suffixExists bool) string {

	if len(s) == 0 {
		return ""
	}

	if suffixExists {
		if s[len(s)-1] == byte('.') {
			return s
		}
		return s + "."
	}

	if s[len(s)-1] == byte('.') {
		return s[0 : len(s)-1]
	}
	return s

}
