package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/shirou/gopsutil/process"
	"os"
	"sync"
)

type internalDaemonStat struct {
	PointsReceived      int64
	PointsReceivedRate  float64
	PointsParseFail     int64
	PointsSoftParseFail int64
	BytesReceived       int64
	ReadFail            int64
	BatchesTransmitted  int64
	BatchesTransmitFail int64
	PointsTransmitted   int64
	OtherErrors         int64
	MemGauge            *process.MemoryInfoStat
	QueueLen            int64
	CPUPercent          float64
}

type DaemonStat struct {
	sync.RWMutex
	*process.Process
	curStat   internalDaemonStat
	savedStat internalDaemonStat
	Interval  int64
}

func (ds *DaemonStat) Init() {
	ds.Process, _ = process.NewProcess(int32(os.Getpid()))
	// start counting CPU usage
	ds.CPUPercent()
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

func (ds *DaemonStat) WriteMetrics(countersMap map[string]int64, gaugesMap map[string]float64, timersMap map[string]Float64Slice, globalPrefix string, metricNamePrefix string, extraTagsStr string) error {

	var ok bool

	if len(metricNamePrefix) == 0 {
		return fmt.Errorf("Empty metric name prefix. No saving to backend")
	}

	// Counters
	pointsReceived := makeBucketName(globalPrefix, metricNamePrefix, "point.received", extraTagsStr)
	_, ok = countersMap[pointsReceived]
	if !ok {
		countersMap[pointsReceived] = 0
	}
	countersMap[pointsReceived] += ds.savedStat.PointsReceived

	pointsParseFail := makeBucketName(globalPrefix, metricNamePrefix, "point.parsefail", extraTagsStr)
	_, ok = countersMap[pointsParseFail]
	if !ok {
		countersMap[pointsParseFail] = 0
	}
	countersMap[pointsParseFail] += ds.savedStat.PointsParseFail

	pointsSoftParseFail := makeBucketName(globalPrefix, metricNamePrefix, "point.softparsefail", extraTagsStr)
	_, ok = countersMap[pointsSoftParseFail]
	if !ok {
		countersMap[pointsSoftParseFail] = 0
	}
	countersMap[pointsSoftParseFail] += ds.savedStat.PointsSoftParseFail

	bytesReceived := makeBucketName(globalPrefix, metricNamePrefix, "read.bytes", extraTagsStr)
	_, ok = countersMap[bytesReceived]
	if !ok {
		countersMap[bytesReceived] = 0
	}
	countersMap[bytesReceived] += ds.savedStat.BytesReceived

	readFail := makeBucketName(globalPrefix, metricNamePrefix, "read.fail", extraTagsStr)
	_, ok = countersMap[readFail]
	if !ok {
		countersMap[readFail] = 0
	}
	countersMap[readFail] += ds.savedStat.ReadFail

	batchesTransmitted := makeBucketName(globalPrefix, metricNamePrefix, "batch.transmitted", extraTagsStr)
	_, ok = countersMap[batchesTransmitted]
	if !ok {
		countersMap[batchesTransmitted] = 0
	}
	countersMap[batchesTransmitted] += ds.savedStat.BatchesTransmitted

	batchesTransmitFail := makeBucketName(globalPrefix, metricNamePrefix, "batch.transmitfail", extraTagsStr)
	_, ok = countersMap[batchesTransmitFail]
	if !ok {
		countersMap[batchesTransmitFail] = 0
	}
	countersMap[batchesTransmitFail] += ds.savedStat.BatchesTransmitFail

	pointsTransmitted := makeBucketName(globalPrefix, metricNamePrefix, "point.transmitted", extraTagsStr)
	_, ok = countersMap[pointsTransmitted]
	if !ok {
		countersMap[pointsTransmitted] = 0
	}
	countersMap[pointsTransmitted] += ds.savedStat.PointsTransmitted

	otherErrors := makeBucketName(globalPrefix, metricNamePrefix, "error.other", extraTagsStr)
	_, ok = countersMap[otherErrors]
	if !ok {
		countersMap[otherErrors] = 0
	}
	countersMap[otherErrors] += ds.savedStat.OtherErrors

	// Gauges
	pointsRate := makeBucketName(globalPrefix, metricNamePrefix, "point.received.rate", extraTagsStr)
	gaugesMap[pointsRate] = ds.savedStat.PointsReceivedRate

	queueLen := makeBucketName(globalPrefix, metricNamePrefix, "queue.len", extraTagsStr)
	gaugesMap[queueLen] = float64(ds.savedStat.QueueLen)

	cpuPercent := makeBucketName(globalPrefix, metricNamePrefix, "cpu.percent", extraTagsStr)
	gaugesMap[cpuPercent] = float64(ds.savedStat.CPUPercent)

	if ds.savedStat.MemGauge != nil {
		memRSS := makeBucketName(globalPrefix, metricNamePrefix, "memory.rss", extraTagsStr)
		gaugesMap[memRSS] = float64(ds.savedStat.MemGauge.RSS)

		memVMS := makeBucketName(globalPrefix, metricNamePrefix, "memory.vms", extraTagsStr)
		gaugesMap[memVMS] = float64(ds.savedStat.MemGauge.VMS)

		memSwap := makeBucketName(globalPrefix, metricNamePrefix, "memory.swap", extraTagsStr)
		gaugesMap[memSwap] = float64(ds.savedStat.MemGauge.Swap)
	}

	return nil

}

func (ds *DaemonStat) ProcessStats() {

	memInfo, _ := ds.Process.MemoryInfo()
	cpuPercent, _ := ds.Process.Percent(0)

	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()

	ds.curStat.MemGauge = memInfo
	ds.curStat.CPUPercent = cpuPercent

	ds.curStat.PointsReceivedRate = float64(ds.curStat.PointsReceived) / float64(ds.Interval)

	// FIXME using GLOBAL channel In
	ds.curStat.QueueLen = int64(len(In))

	ds.savedStat = ds.curStat
	ds.curStat = internalDaemonStat{}

}

func makeBucketName(globalPrefix string, metricNamePrefix string, metricName string, extraTagsStr string) string {

	if len(globalPrefix) == 0 && len(metricNamePrefix) == 0 && len(extraTagsStr) == 0 {
		return metricName
	}

	sep := ""
	if len(extraTagsStr) > 0 {
		sep = "^"
	}
	return normalizeDot(globalPrefix, true) + normalizeDot(metricNamePrefix, true) + normalizeDot(metricName, len(extraTagsStr) > 0) + sep + normalizeDot(extraTagsStr, false)
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
