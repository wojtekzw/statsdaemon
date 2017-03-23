package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/patrickmn/go-cache"
	"github.com/shirou/gopsutil/process"
	"os"
	"runtime"
	"sync"
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
	MemGauge               *process.MemoryInfoStat
	QueueLen               int64
	CPUPercent             float64
	PacketCacheHit         int64
	PacketCacheMiss        int64
	PacketCacheSize        int64
	NameCacheHit           int64
	NameCacheMiss          int64
	NameCacheSize          int64
	Goroutines             int
}

type DaemonStat struct {
	sync.RWMutex
	*process.Process
	curStat   internalDaemonStat
	savedStat internalDaemonStat
	Interval  int64
}

func (ds *DaemonStat) Init(q chan *Packet, t time.Duration, interval int64) {
	// batch send interval
	ds.Interval = interval

	ds.Process, _ = process.NewProcess(int32(os.Getpid()))
	// start counting CPU usage
	ds.CPUPercent()

	// start background incoming queue len monitoring every t time
	go ds.QueueStats(q, t)

	// monitor number of goroutines
	go ds.GoroutinesStats(t)

}

func (ds *DaemonStat) CPUPercent() {
	cpuPerc, _ := ds.Process.Percent(0)
	ds.RWMutex.Lock()
	ds.curStat.CPUPercent = cpuPerc
	ds.RWMutex.Unlock()
	log.Errorf("CPUPercent: %.1f%%", ds.curStat.CPUPercent)

}
func (ds *DaemonStat) GlobalVarsSizeToString() string {
	s := fmt.Sprintf("counters: %d, gauges: %d, lastGaugeValue: %d, timers: %d, countInactivity: %d, sets: %d, keys: %d",
		len(counters), len(gauges), len(lastGaugeValue), len(timers), len(countInactivity), len(sets), len(keys))
	return s
}

func (ds *DaemonStat) String() string {
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

	if ds.savedStat.MemGauge != nil {
		s = s + fmt.Sprintf("MemRSS: %.0f MB, ", float64(ds.savedStat.MemGauge.RSS)/(1024*1024))
		s = s + fmt.Sprintf("MemVMS: %.0f MB, ", float64(ds.savedStat.MemGauge.VMS)/(1024*1024))
		s = s + fmt.Sprintf("MemSwap: %.0f MB, ", float64(ds.savedStat.MemGauge.Swap)/(1024*1024))
	}
	s = s + fmt.Sprintf("CPUPercent: %.1f%%, ", ds.savedStat.CPUPercent)
	s = s + fmt.Sprintf("QueueLen: %d, ", ds.savedStat.QueueLen)
	s = s + ds.GlobalVarsSizeToString()
	return s
}

func (ds *DaemonStat) Print() {
	fmt.Printf("%s", ds.String())
}

func (ds *DaemonStat) PointsReceivedInc() {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.PointsReceived++
}

func (ds *DaemonStat) OtherErrorsInc() {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.OtherErrors++

}

func (ds *DaemonStat) BytesReceivedInc(n int64) {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.BytesReceived += n
}

func (ds *DaemonStat) ReadFailInc() {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.ReadFail++
}

func (ds *DaemonStat) PointsParseFailInc() {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.PointsParseFail++
}

func (ds *DaemonStat) PointsParseSoftFailInc() {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.PointsSoftParseFail++
}

func (ds *DaemonStat) BatchesTransmittedInc() {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.BatchesTransmitted++
}

func (ds *DaemonStat) BatchesTransmitFailInc() {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.BatchesTransmitFail++
}

func (ds *DaemonStat) PointsTransmittedInc(n int64) {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.PointsTransmitted += n
}

func (ds *DaemonStat) PointTypeInc(p *Packet) {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()

	switch p.Modifier {
	case "c":
		ds.curStat.PointsReceivedCounter++
	case "g":
		ds.curStat.PointsReceivedGauge++
	case "s":
		ds.curStat.PointsReceivedSet++
	case "ms":
		ds.curStat.PointsReceivedTimer++
	case "kv":
		ds.curStat.PointsReceivedKeyValue++
	}
}

func (ds *DaemonStat) PacketCacheHit() {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.PacketCacheHit++
}

func (ds *DaemonStat) PacketCacheMiss() {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.PacketCacheMiss++
}

func (ds *DaemonStat) PacketCacheSize(c *cache.Cache) {
	ds.curStat.PacketCacheSize = int64(c.ItemCount())
}

func (ds *DaemonStat) NameCacheHit() {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.NameCacheHit++
}

func (ds *DaemonStat) NameCacheMiss() {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.NameCacheMiss++
}

func (ds *DaemonStat) NameCacheSize(c *cache.Cache) {
	ds.curStat.NameCacheSize = int64(c.ItemCount())
}

func (ds *DaemonStat) WriteMetrics(countersMap map[string]int64, gaugesMap map[string]float64, timersMap map[string]Float64Slice, globalPrefix string, metricNamePrefix string, extraTagsStr string) error {

	var ok bool

	if len(metricNamePrefix) == 0 {
		return fmt.Errorf("Empty metric name prefix. No saving to backend")
	}

	versionTag := "statsdaemon="+StatsdaemonVersion
	// Counters
	versionCounter := makeBucketName(globalPrefix, metricNamePrefix, "version.counter", extraTagsStr,versionTag)
	countersMap[versionCounter] = 1


	pointsReceived := makeBucketName(globalPrefix, metricNamePrefix, "point.received", extraTagsStr,versionTag)
	_, ok = countersMap[pointsReceived]
	if !ok {
		countersMap[pointsReceived] = 0
	}
	countersMap[pointsReceived] += ds.savedStat.PointsReceived

	pointsParseFail := makeBucketName(globalPrefix, metricNamePrefix, "point.parsefail", extraTagsStr,versionTag)
	_, ok = countersMap[pointsParseFail]
	if !ok {
		countersMap[pointsParseFail] = 0
	}
	countersMap[pointsParseFail] += ds.savedStat.PointsParseFail

	pointsSoftParseFail := makeBucketName(globalPrefix, metricNamePrefix, "point.softparsefail", extraTagsStr,versionTag)
	_, ok = countersMap[pointsSoftParseFail]
	if !ok {
		countersMap[pointsSoftParseFail] = 0
	}
	countersMap[pointsSoftParseFail] += ds.savedStat.PointsSoftParseFail

	bytesReceived := makeBucketName(globalPrefix, metricNamePrefix, "read.bytes", extraTagsStr,versionTag)
	_, ok = countersMap[bytesReceived]
	if !ok {
		countersMap[bytesReceived] = 0
	}
	countersMap[bytesReceived] += ds.savedStat.BytesReceived

	readFail := makeBucketName(globalPrefix, metricNamePrefix, "read.fail", extraTagsStr,versionTag)
	_, ok = countersMap[readFail]
	if !ok {
		countersMap[readFail] = 0
	}
	countersMap[readFail] += ds.savedStat.ReadFail

	batchesTransmitted := makeBucketName(globalPrefix, metricNamePrefix, "batch.transmitted", extraTagsStr,versionTag)
	_, ok = countersMap[batchesTransmitted]
	if !ok {
		countersMap[batchesTransmitted] = 0
	}
	countersMap[batchesTransmitted] += ds.savedStat.BatchesTransmitted

	batchesTransmitFail := makeBucketName(globalPrefix, metricNamePrefix, "batch.transmitfail", extraTagsStr,versionTag)
	_, ok = countersMap[batchesTransmitFail]
	if !ok {
		countersMap[batchesTransmitFail] = 0
	}
	countersMap[batchesTransmitFail] += ds.savedStat.BatchesTransmitFail

	pointsTransmitted := makeBucketName(globalPrefix, metricNamePrefix, "point.transmitted", extraTagsStr,versionTag)
	_, ok = countersMap[pointsTransmitted]
	if !ok {
		countersMap[pointsTransmitted] = 0
	}
	countersMap[pointsTransmitted] += ds.savedStat.PointsTransmitted

	otherErrors := makeBucketName(globalPrefix, metricNamePrefix, "error.other", extraTagsStr,versionTag)
	_, ok = countersMap[otherErrors]
	if !ok {
		countersMap[otherErrors] = 0
	}
	countersMap[otherErrors] += ds.savedStat.OtherErrors

	pointsReceivedCounter := makeBucketName(globalPrefix, metricNamePrefix, "point.received.counter", extraTagsStr,versionTag)
	_, ok = countersMap[pointsReceivedCounter]
	if !ok {
		countersMap[pointsReceivedCounter] = 0
	}
	countersMap[pointsReceivedCounter] += ds.savedStat.PointsReceivedCounter

	pointsReceivedGauge := makeBucketName(globalPrefix, metricNamePrefix, "point.received.gauge", extraTagsStr,versionTag)
	_, ok = countersMap[pointsReceivedGauge]
	if !ok {
		countersMap[pointsReceivedGauge] = 0
	}
	countersMap[pointsReceivedGauge] += ds.savedStat.PointsReceivedGauge

	pointsReceivedSet := makeBucketName(globalPrefix, metricNamePrefix, "point.received.set", extraTagsStr,versionTag)
	_, ok = countersMap[pointsReceivedSet]
	if !ok {
		countersMap[pointsReceivedSet] = 0
	}
	countersMap[pointsReceivedSet] += ds.savedStat.PointsReceivedSet

	pointsReceivedTimer := makeBucketName(globalPrefix, metricNamePrefix, "point.received.timer", extraTagsStr,versionTag)
	_, ok = countersMap[pointsReceivedTimer]
	if !ok {
		countersMap[pointsReceivedTimer] = 0
	}
	countersMap[pointsReceivedTimer] += ds.savedStat.PointsReceivedTimer

	pointsReceivedKeyValue := makeBucketName(globalPrefix, metricNamePrefix, "point.received.keyvalue", extraTagsStr,versionTag)
	_, ok = countersMap[pointsReceivedKeyValue]
	if !ok {
		countersMap[pointsReceivedKeyValue] = 0
	}
	countersMap[pointsReceivedKeyValue] += ds.savedStat.PointsReceivedKeyValue

	packetCacheHit := makeBucketName(globalPrefix, metricNamePrefix, "cache.packet.hit", extraTagsStr,versionTag)
	_, ok = countersMap[packetCacheHit]
	if !ok {
		countersMap[packetCacheHit] = 0
	}
	countersMap[packetCacheHit] += ds.savedStat.PacketCacheHit

	packetCacheMiss := makeBucketName(globalPrefix, metricNamePrefix, "cache.packet.miss", extraTagsStr,versionTag)
	_, ok = countersMap[packetCacheMiss]
	if !ok {
		countersMap[packetCacheMiss] = 0
	}
	countersMap[packetCacheMiss] += ds.savedStat.PacketCacheMiss

	nameCacheHit := makeBucketName(globalPrefix, metricNamePrefix, "cache.name.hit", extraTagsStr,versionTag)
	_, ok = countersMap[nameCacheHit]
	if !ok {
		countersMap[nameCacheHit] = 0
	}
	countersMap[nameCacheHit] += ds.savedStat.NameCacheHit

	nameCacheMiss := makeBucketName(globalPrefix, metricNamePrefix, "cache.name.miss", extraTagsStr,versionTag)
	_, ok = countersMap[nameCacheMiss]
	if !ok {
		countersMap[nameCacheMiss] = 0
	}
	countersMap[nameCacheMiss] += ds.savedStat.NameCacheMiss

	// Gauges
	pointsRate := makeBucketName(globalPrefix, metricNamePrefix, "point.received.rate", extraTagsStr,versionTag)
	gaugesMap[pointsRate] = ds.savedStat.PointsReceivedRate

	queueLen := makeBucketName(globalPrefix, metricNamePrefix, "queue.len", extraTagsStr,versionTag)
	gaugesMap[queueLen] = float64(ds.savedStat.QueueLen)

	cpuPercent := makeBucketName(globalPrefix, metricNamePrefix, "cpu.percent", extraTagsStr,versionTag)
	gaugesMap[cpuPercent] = float64(ds.savedStat.CPUPercent)

	packetCacheSize := makeBucketName(globalPrefix, metricNamePrefix, "cache.packet.size", extraTagsStr,versionTag)
	gaugesMap[packetCacheSize] = float64(ds.savedStat.PacketCacheSize)

	nameCacheSize := makeBucketName(globalPrefix, metricNamePrefix, "cache.name.size", extraTagsStr,versionTag)
	gaugesMap[nameCacheSize] = float64(ds.savedStat.NameCacheSize)

	goroutines := makeBucketName(globalPrefix, metricNamePrefix, "goroutines.number", extraTagsStr,versionTag)
	gaugesMap[goroutines] = float64(ds.savedStat.Goroutines)

	if ds.savedStat.MemGauge != nil {
		memRSS := makeBucketName(globalPrefix, metricNamePrefix, "memory.rss", extraTagsStr,versionTag)
		gaugesMap[memRSS] = float64(ds.savedStat.MemGauge.RSS)

		memVMS := makeBucketName(globalPrefix, metricNamePrefix, "memory.vms", extraTagsStr,versionTag)
		gaugesMap[memVMS] = float64(ds.savedStat.MemGauge.VMS)

		memSwap := makeBucketName(globalPrefix, metricNamePrefix, "memory.swap", extraTagsStr,versionTag)
		gaugesMap[memSwap] = float64(ds.savedStat.MemGauge.Swap)
	}

	return nil

}

// QueueStats - incoming queue len monitoring every t time
func (ds *DaemonStat) QueueStats(c chan *Packet, t time.Duration) {

	for {
		l := int64(len(c))

		ds.RWMutex.Lock()
		if l > ds.curStat.QueueLen {
			ds.curStat.QueueLen = l
		}
		ds.RWMutex.Unlock()

		time.Sleep(t)

	}

}

// GoroutinesStats - max number of goroutines monitored  every t time
func (ds *DaemonStat) GoroutinesStats(t time.Duration) {

	for {
		n := runtime.NumGoroutine()

		ds.RWMutex.Lock()
		if n > ds.curStat.Goroutines {
			ds.curStat.Goroutines = n
		}
		ds.RWMutex.Unlock()

		time.Sleep(t)

	}

}

func (ds *DaemonStat) ProcessStats(packetCache, nameCache *cache.Cache) {

	memInfo, _ := ds.Process.MemoryInfo()
	cpuPercent, _ := ds.Process.Percent(0)
	nameSize := int64(nameCache.ItemCount())
	packetSize := int64(packetCache.ItemCount())

	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()

	ds.curStat.MemGauge = memInfo
	ds.curStat.CPUPercent = cpuPercent
	ds.curStat.NameCacheSize = nameSize
	ds.curStat.PacketCacheSize = packetSize
	ds.curStat.PointsReceivedRate = float64(ds.curStat.PointsReceived) / float64(ds.Interval)

	ds.savedStat = ds.curStat
	ds.curStat = internalDaemonStat{}

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

	return normalizeDot(globalPrefix, true) + normalizeDot(metricNamePrefix, true) + normalizeDot(metricName, len(extraTagsStr) > 0 || len(addTagsStr) > 0 ) +
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
