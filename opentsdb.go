// opentsdb.go
package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/mapmyfitness/go-opentsdb/tsdb"
)

func openTSDB(config ConfigApp, buffer *bytes.Buffer) error {
	logCtx := log.WithFields(log.Fields{
		"in":  "openTSDB",
		"ctx": "Metric format error",
	})

	if config.OpenTSDBAddress != "-" && config.OpenTSDBAddress != "" {
		datapoints := []tsdb.DataPoint{}
		TSDB := tsdb.TSDB{}
		server := tsdb.Server{}
		serverAdress := strings.Split(config.OpenTSDBAddress, ":")
		if len(serverAdress) != 2 {
			return fmt.Errorf("Incorrect openTSDB server address %v", serverAdress)
		}
		port, err := strconv.ParseUint(serverAdress[1], 10, 32)
		if err != nil {
			return err
		}
		server.Host = serverAdress[0]
		server.Port = uint(port)
		metrics := strings.Split(buffer.String(), "\n")
		metrics = removeEmptyLines(metrics)
		num := len(metrics)
		for _, mtr := range metrics {
			// target format: cpu.load 12.50 112345566 host=dev,zone=west
			data := strings.Split(mtr, " ")
			if len(data) >= 3 {
				metric := tsdb.Metric{}
				value := tsdb.Value{}
				tags := tsdb.Tags{}
				timestamp := tsdb.Time{}
				datapoint := tsdb.DataPoint{}

				// parse value
				// K/V values NOT allowed for OpenTSDB as metric
				// TODO - consider setting them as tags
				val, err := strconv.ParseFloat(data[1], 64)
				if err != nil {
					// continue on error in one metric
					logCtx.WithField("after", "ParseFloat").Errorf("Only float/integer values allowed. Got: %s in line \"%s\". Error: %s", data[1], data, err)
					continue
				}
				value.Set(val)

				// parse timestamp
				err = timestamp.Parse(data[2])
				if err != nil {
					logCtx.WithField("after", "Parse").Errorf("Timestamp expected. Got: %s in line \"%s\". Error: %s", data[2], data, err)
					continue
				}

				// parse metric/bucket
				metricName := data[0]
				err = metric.Set(metricName)
				if err != nil {
					logCtx.WithField("after", "Set").Errorf("Metric name expected. Got: %s in line \"%s\". Error: %s", data[0], data, err)
					continue
				}

				if len(data) == 4 {
					combinedTagsSlice := strings.Split(data[3], ",")
					if len(combinedTagsSlice) > 0 {
						for _, e := range combinedTagsSlice {
							strSlice := strings.Split(e, "=")
							if len(strSlice) == 2 {
								if strSlice[0] == "" || strSlice[1] == "" {
									logCtx.WithField("after", "Split").Errorf("Tag  expected. Got: %s in line \"%s\"", e, data)
									continue
								}
								tags.Set(strSlice[0], strSlice[1])
							}
						}
					}
				}

				datapoint.Value = &value
				datapoint.Metric = &metric
				datapoint.Tags = &tags
				datapoint.Timestamp = &timestamp
				datapoints = append(datapoints, datapoint)
			} else {
				logCtx.WithField("after", "Split").Errorf("Buffer format. Expected \"metric value timestamp\". Got \"%s\"", mtr)
			}

		}

		TSDB.Servers = append(TSDB.Servers, server)
		_, err = TSDB.Put(datapoints)
		if err != nil {
			return err
		}
		logCtx.WithField("after", "Put").Infof("sent %d stats to %s", num, config.OpenTSDBAddress)

		return nil

	}
	return fmt.Errorf("No valid OpenTSDB address: %s", config.OpenTSDBAddress)

}
