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
	s := fmt.Sprintf("counters: %d, gauges: %d (%#v), lastGaugeValue: %d, lastGaugeTags: %d, timers: %d, countInactivity: %d, sets: %d, keys: %d, tags: %d (%#v)",
		len(counters), len(gauges), gauges, len(lastGaugeValue), len(lastGaugeTags), len(timers), len(countInactivity), len(sets), len(keys), len(tags), tags)
	return s
}
func (ds *DaemonStat) String() string {

	s := fmt.Sprintf("PointsCounter: %d ops, ", ds.PointsCounter)
	s = s + fmt.Sprintf("PointsRate: %.2f ops/s, ", ds.PointsRate)
	s = s + fmt.Sprintf("ErrorsCounter: %d errors, ", ds.ErrorsCounter)
	s = s + fmt.Sprintf("MemRES: %.0f MB, ", float64(ds.MemGauge.RSS)/(1024*1024))
	s = s + fmt.Sprintf("MemVIRT: %.0f MB, ", float64(ds.MemGauge.VMS)/(1024*1024))
	s = s + fmt.Sprintf("MemSwap: %.0f MB, ", float64(ds.MemGauge.Swap)/(1024*1024))
	s = s + fmt.Sprintf("QueueLen: %d, ", ds.QueueLen)
	s = s + ds.GlobalVarsSizeToString()
	return s
}

func (ds *DaemonStat) Print() {
	fmt.Printf("%s", ds.String())
}

func (ds *DaemonStat) PointIncr() {
	ds.PointsCounter++
}

func (ds *DaemonStat) ErrorIncr() {
	ds.ErrorsCounter++
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
