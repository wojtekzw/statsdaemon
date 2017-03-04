package main

import (
	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/shirou/gopsutil/process"
	"sync"
)

type internalDaemonStat struct {
	PointsCounter int64
	PointsRate    float64
	ErrorsCounter int64
	MemGauge      *process.MemoryInfoStat
	QueueLen      int64
}

type DaemonStat struct {
	sync.RWMutex
	curStat internalDaemonStat
	savedStat internalDaemonStat
	Interval      int64
}

func (ds *DaemonStat) GlobalVarsSizeToString() string {
	s := fmt.Sprintf("counters: %d, gauges: %d, lastGaugeValue: %d, timers: %d, countInactivity: %d, sets: %d, keys: %d",
		len(counters), len(gauges), len(lastGaugeValue), len(timers), len(countInactivity), len(sets), len(keys))
	return s
}


func (ds *DaemonStat) String() string {
	s := fmt.Sprintf("PointsCounter: %d ops, ", ds.savedStat.PointsCounter)
	s = s + fmt.Sprintf("PointsRate: %.2f ops/s, ", ds.savedStat.PointsRate)
	s = s + fmt.Sprintf("ErrorsCounter: %d errors, ", ds.savedStat.ErrorsCounter)

	if ds.savedStat.MemGauge != nil {
		s = s + fmt.Sprintf("MemRSS: %.0f MB, ", float64(ds.savedStat.MemGauge.RSS)/(1024*1024))
		s = s + fmt.Sprintf("MemVMS: %.0f MB, ", float64(ds.savedStat.MemGauge.VMS)/(1024*1024))
		s = s + fmt.Sprintf("MemSwap: %.0f MB, ", float64(ds.savedStat.MemGauge.Swap)/(1024*1024))
	}
	s = s + fmt.Sprintf("QueueLen: %d, ", ds.savedStat.QueueLen)
	s = s + ds.GlobalVarsSizeToString()
	return s
}

func (ds *DaemonStat) Print() {
	fmt.Printf("%s", ds.String())
}

func (ds *DaemonStat) PointIncr() {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.PointsCounter++
	if ds.curStat.PointsCounter < 0 {
		ds.curStat.PointsCounter = 0
	}
}

func (ds *DaemonStat) ErrorIncr() {
	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()
	ds.curStat.ErrorsCounter++
	if ds.curStat.ErrorsCounter < 0 {
		ds.curStat.ErrorsCounter = 0
	}
}

func (ds *DaemonStat) WriteMetrics(countersMap map[string]int64, gaugesMap map[string]float64, timersMap map[string]Float64Slice, globalPrefix string, metricNamePrefix string, extraTagsStr string) error {

	var ok bool

	if len(metricNamePrefix) == 0 {
		return fmt.Errorf("Empty metric name prefix. No saving to backend")
	}
	// Counters
	pointsCounter := makeBucketName(globalPrefix, metricNamePrefix, "point.count", extraTagsStr)
	_, ok = countersMap[pointsCounter]
	if !ok {
		countersMap[pointsCounter] = 0
	}
	countersMap[pointsCounter] += ds.savedStat.PointsCounter

	errorsCounter := makeBucketName(globalPrefix, metricNamePrefix, "error.count", extraTagsStr)
	_, ok = countersMap[errorsCounter]
	if !ok {
		countersMap[errorsCounter] = 0
	}
	countersMap[errorsCounter] += ds.savedStat.ErrorsCounter

	// Gauges
	pointsRate := makeBucketName(globalPrefix, metricNamePrefix, "point.rate", extraTagsStr)
	gaugesMap[pointsRate] = ds.savedStat.PointsRate

	queueLen := makeBucketName(globalPrefix, metricNamePrefix, "queue.len", extraTagsStr)
	gaugesMap[queueLen] = float64(ds.savedStat.QueueLen)

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

	p, err := process.NewProcess(int32(os.Getpid()))

	ds.RWMutex.Lock()
	defer ds.RWMutex.Unlock()

	if err == nil {
		ds.curStat.MemGauge, _ = p.MemoryInfo()
	} else {
		ds.curStat.MemGauge = nil
		log.Errorf("%v", err)
		Stat.ErrorIncr()
	}

	ds.curStat.PointsRate = float64(ds.curStat.PointsCounter) / float64(ds.Interval)
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
