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
		"in": "submit",
	})

	// Prepare internal stats (make a copy, reset current counters)
	Stat.ProcessStats(packetCache,nameCache)

	if !Config.DisableStatSend {
		Stat.WriteMetrics(counters, gauges, timers, "", Config.StatsPrefix, normalizeTags(Config.ExtraTagsHash, tfDefault))
	}
	if Config.InternalLogLevel >= log.DebugLevel {
		logCtx.Debugf("%s", Stat.String())
	}

	// Universal format in buffer
	num += processCounters(&buffer, now, Config.ResetCounters, backend, dbHandle)
	num += processGauges(&buffer, now, backend)
	num += processTimers(&buffer, now, Config.PercentThreshold, backend)
	num += processSets(&buffer, now, backend)
	num += processKeyValue(&buffer, now, backend)

	Stat.PointsTransmittedInc(num)

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
				Stat.BatchesTransmitFailInc()
			} else {
				Stat.BatchesTransmittedInc()
			}
		} else {
			if err := sendDataStdout(&buffer); err != nil {
				logCtx.Errorf("%s", err)
				Stat.BatchesTransmitFailInc()
			} else {
				Stat.BatchesTransmittedInc()
			}
		}

	case "graphite":
		err := graphite(Config, deadline, &buffer)
		if err != nil {
			logCtx.Errorf("%s", err)
			Stat.BatchesTransmitFailInc()
		} else {
			Stat.BatchesTransmittedInc()
		}

	case "opentsdb":
		err := openTSDB(Config, &buffer)
		if err != nil {
			logCtx.Errorf("%s", err)
			Stat.BatchesTransmitFailInc()
		} else {
			Stat.BatchesTransmittedInc()
		}

	case "dummy":
	//	do nothing

	default:
		logCtx.Fatalf("Invalid backend %s. Exiting...\n", backend)
	}

	return nil
}
