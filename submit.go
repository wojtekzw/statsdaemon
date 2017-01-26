package main

import (
	"bytes"
	"time"

	log "github.com/Sirupsen/logrus"
)

func submit(deadline time.Time, backend string) error {
	var buffer bytes.Buffer
	var num int64

	now := time.Now().Unix()
	logCtx := log.WithFields(log.Fields{
		"in":  "submit",
	})

	// Prepare internal stats (queueLen is set at the end of func)
	Stat.ProcessStats()
	if !Config.DisableStatSend {
		Stat.WriteMetrics(counters, gauges, timers, "", Config.StatsPrefix, normalizeTags(Config.ExtraTagsHash, tfDefault))
	}
	if Config.InternalLogLevel >= log.DebugLevel {
		logCtx.Debugf("%s", Stat.String())
	}

	Stat.PointsCounter = 0 // reset counter after each interval

	// fmt.Printf("Len size - start submit: %d\n", len(In))
	// Universal format in buffer
	num += processCounters(&buffer, now, Config.ResetCounters, backend, dbHandle)
	num += processGauges(&buffer, now, backend)
	num += processTimers(&buffer, now, Config.PercentThreshold, backend)
	num += processSets(&buffer, now, backend)
	num += processKeyValue(&buffer, now, backend)

	if num == 0 {
		Stat.QueueLen = int64(len(In))
		return nil
	}

	if Config.InternalLogLevel >= log.DebugLevel {
		for _, line := range bytes.Split(buffer.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			logCtx.Debugf("Metrics to backend: %s", line)
		}
	}

	// send stats to backend
	switch backend {
	case "external":
		if Config.PostFlushCmd != "stdout" {
			err := sendDataExtCmd(Config.PostFlushCmd, &buffer)
			if err != nil {
				logCtx.Errorf("%s", err)
				Stat.ErrorIncr()
			}
		} else {
			if err := sendDataStdout(&buffer); err != nil {
				logCtx.Errorf("%s", err)
				Stat.ErrorIncr()
			}
		}

	case "graphite":
		err := graphite(Config, deadline, &buffer)
		if err != nil {
			logCtx.Errorf("%s", err)
			Stat.ErrorIncr()
		}

	case "opentsdb":
		err := openTSDB(Config, &buffer)
		if err != nil {
			logCtx.Errorf("%s", err)
			Stat.ErrorIncr()
		}

	case "dummy":
	//	do nothing

	default:
		logCtx.Fatalf("Invalid backend %s. Exiting...\n", backend)
	}

	//  Internal stats
	Stat.QueueLen = int64(len(In))

	return nil
}
