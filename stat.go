package main

import (
	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/shirou/gopsutil/process"
)

type DaemonStat struct {
	PointsCounter int64
	PointsRate    float64
	ErrorsCounter int64
	MemGauge      *process.MemoryInfoStat
	QueueLen      int64
	Interval      int64
}

func (ds *DaemonStat) GlobalVarsSizeToString() string {
	s := fmt.Sprintf("counters: %d, gauges: %d, lastGaugeValue: %d, timers: %d, countInactivity: %d, sets: %d, keys: %d",
		len(counters), len(gauges), len(lastGaugeValue), len(timers), len(countInactivity), len(sets), len(keys))
	return s
}
func (ds *DaemonStat) String() string {

	s := fmt.Sprintf("PointsCounter: %d ops, ", ds.PointsCounter)
	s = s + fmt.Sprintf("PointsRate: %.2f ops/s, ", ds.PointsRate)
	s = s + fmt.Sprintf("ErrorsCounter: %d errors, ", ds.ErrorsCounter)

	if ds.MemGauge != nil {
		s = s + fmt.Sprintf("MemRSS: %.0f MB, ", float64(ds.MemGauge.RSS)/(1024*1024))
		s = s + fmt.Sprintf("MemVMS: %.0f MB, ", float64(ds.MemGauge.VMS)/(1024*1024))
		s = s + fmt.Sprintf("MemSwap: %.0f MB, ", float64(ds.MemGauge.Swap)/(1024*1024))
	}
	s = s + fmt.Sprintf("QueueLen: %d, ", ds.QueueLen)
	s = s + ds.GlobalVarsSizeToString()
	return s
}

func (ds *DaemonStat) Print() {
	fmt.Printf("%s", ds.String())
}

func (ds *DaemonStat) PointIncr() {
	ds.PointsCounter++
	if ds.PointsCounter < 0 {
		ds.PointsCounter = 0
	}
}

func (ds *DaemonStat) ErrorIncr() {
	ds.ErrorsCounter++
	if ds.ErrorsCounter < 0 {
		ds.ErrorsCounter = 0
	}
}

func (ds *DaemonStat) WriteMerics(countersMap map[string]int64, gaugesMap map[string]float64, timersMap map[string]Float64Slice, globalPrefix string, metricNamePrefix string, extraTagsStr string) error {

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
	countersMap[pointsCounter] += ds.PointsCounter

	errorsCounter := makeBucketName(globalPrefix, metricNamePrefix, "error.count", extraTagsStr)
	_, ok = countersMap[errorsCounter]
	if !ok {
		countersMap[errorsCounter] = 0
	}
	countersMap[errorsCounter] += ds.ErrorsCounter

	// Gauges
	pointsRate := makeBucketName(globalPrefix, metricNamePrefix, "point.rate", extraTagsStr)
	gaugesMap[pointsRate] = ds.PointsRate

	queueLen := makeBucketName(globalPrefix, metricNamePrefix, "queue.len", extraTagsStr)
	gaugesMap[queueLen] = float64(ds.QueueLen)

	if ds.MemGauge != nil {
		memRSS := makeBucketName(globalPrefix, metricNamePrefix, "memory.rss", extraTagsStr)
		gaugesMap[memRSS] = float64(ds.MemGauge.RSS)

		memVMS := makeBucketName(globalPrefix, metricNamePrefix, "memory.vms", extraTagsStr)
		gaugesMap[memVMS] = float64(ds.MemGauge.VMS)

		memSwap := makeBucketName(globalPrefix, metricNamePrefix, "memory.swap", extraTagsStr)
		gaugesMap[memSwap] = float64(ds.MemGauge.Swap)
	}

	return nil

}

func (ds *DaemonStat) ProcessStats() {
	ds.PointsRate = float64(ds.PointsCounter) / float64(ds.Interval)
	p, err := process.NewProcess(int32(os.Getpid()))
	if err == nil {
		ds.MemGauge, _ = p.MemoryInfo()
	} else {
		ds.MemGauge = nil
		log.Errorf("%v", err)
		Stat.ErrorIncr()
	}
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
