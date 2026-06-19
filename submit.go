package main

import (
	"bytes"
	"time"

	"fmt"

	log "github.com/sirupsen/logrus"
)

func submit(mx *metrics, deadline time.Time) error {

	backend := Config.BackendType
	var buffer bytes.Buffer
	var num int64

	now := time.Now().Unix()
	logCtx := log.WithFields(log.Fields{
		"in": "submit",
	})

	// Prepare internal stats (make a copy, reset current counters)
	Stat.ProcessStats(packetCache, nameCache)

	if !Config.DisableStatSend {
		Stat.WriteMetrics(mx.counters, mx.gauges, mx.timers, "", Config.StatsPrefix, normalizeTags(Config.ExtraTagsHash, tfDefault))
	}
	if Config.InternalLogLevel >= log.DebugLevel {
		logCtx.Debugf("%s", Stat.String(mx))
	}

	// Universal format in buffer
	num += mx.processCounters(&buffer, now, Config.ResetCounters, backend, dbHandle)
	num += mx.processGauges(&buffer, now, backend)
	num += mx.processTimers(&buffer, now, Config.PercentThreshold, backend)
	num += mx.processSets(&buffer, now, backend)
	num += mx.processKeyValue(&buffer, now, backend)

	Stat.PointsTransmittedInc(num)

	if Config.InternalLogLevel >= log.DebugLevel || Config.CfgDebugMetrics.Enabled {
		for _, line := range bytes.Split(buffer.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			if Config.InternalLogLevel >= log.DebugLevel {
				logCtx.Debugf("Metrics to backend: %s", line)
			}
			if Config.CfgDebugMetrics.Enabled {
				if prefixPresent(string(line), Config.CfgDebugMetrics.Patterns) {
					fmt.Fprintf(Config.CfgDebugMetrics.LogFile, "%s OUT: %s\n", time.Now().Format(time.RFC3339), string(line))
				}
			}
		}
	}

	// send stats to backend
	b, err := selectBackend(Config)
	if err != nil {
		fmt.Printf("%s. Exiting...\n", err)
		logCtx.Fatalf("%s. Exiting...", err)
	}
	if b != nil { // nil == dummy backend (no-op)
		if err := b.Send(&buffer, deadline); err != nil {
			logCtx.Errorf("%s", err)
			Stat.BatchesTransmitFailInc()
		} else {
			Stat.BatchesTransmittedInc()
		}
	}

	return nil
}
