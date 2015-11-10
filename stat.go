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

func (ds *DaemonStat) String() string {

	s := fmt.Sprintf("PointsCounter: %d ops\n", ds.PointsCounter)
	s = s + fmt.Sprintf("PointsRate: %.2f ops/s\n", ds.PointsRate)
	s = s + fmt.Sprintf("ErrorsCounter: %d errors\n", ds.ErrorsCounter)
	s = s + fmt.Sprintf("MemRES: %.0f MB\n", float64(ds.MemGauge.RSS)/(1024*1024))
	s = s + fmt.Sprintf("MemVIRT: %.0f MB\n", float64(ds.MemGauge.VMS)/(1024*1024))
	s = s + fmt.Sprintf("MemSwap: %.0f MB\n", float64(ds.MemGauge.Swap)/(1024*1024))
	s = s + fmt.Sprintf("QueueLen: %d\n", ds.QueueLen)
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
