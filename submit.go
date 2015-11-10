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
		"ctx": "Buffer send to backend",
	})

	// fmt.Printf("Len size - start submit: %d\n", len(In))
	// Universal format in buffer
	num += processCounters(&buffer, now, Config.ResetCounters, backend, dbHandle)
	num += processGauges(&buffer, now, backend)
	num += processTimers(&buffer, now, Config.PercentThreshold, backend)
	num += processSets(&buffer, now, backend)
	num += processKeyValue(&buffer, now, backend)

	if num == 0 {
		return nil
	}

	if Config.InternalLogLevel == log.DebugLevel {
		for _, line := range bytes.Split(buffer.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			logCtx.WithField("after", "Processing metrics").Debugf("Output line: %s", line)
		}
	}

	// send stats to backend
	switch backend {
	case "external":
		if Config.PostFlushCmd != "stdout" {
			err := sendDataExtCmd(Config.PostFlushCmd, &buffer)
			if err != nil {
				logCtx.WithField("after", "sendDataExtCmd").Errorf("%s", err)
				Stat.ErrorIncr()
			}
			logCtx.WithField("after", "sendDataExtCmd").Infof("sent %d stats to external command", num)
		} else {
			if err := sendDataStdout(&buffer); err != nil {
				logCtx.WithField("after", "sendDataStdout").Errorf("%s", err)
				Stat.ErrorIncr()
			}
		}

	case "graphite":
		err := graphite(Config, deadline, &buffer)
		if err != nil {
			logCtx.WithField("after", "graphite").Errorf("%s", err)
			Stat.ErrorIncr()
		}
		logCtx.WithField("after", "graphite").Infof("wrote %d stats to graphite(%s)", num, Config.GraphiteAddress)

	case "opentsdb":
		err := openTSDB(Config, &buffer)
		if err != nil {
			logCtx.WithField("after", "openTSDB").Errorf("%s", err)
			Stat.ErrorIncr()
		}

	case "dummy":

	default:
		logCtx.WithField("after", "backends").Fatalf("Invalid backend %s. Exiting...\n", backend)
	}

	// fmt.Printf("Len size - end submit: %d\n", len(In))
	// Stat section
	Stat.QueueLen = int64(len(In))
	Stat.ProcessStats()
	logCtx.WithField("after", "submit").Infof("%s", Stat.String())
	Stat.PointsCounter = 0
	Stat.ErrorsCounter = 0

	return nil
}
