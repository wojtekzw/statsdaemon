// opentsdb.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/wojtekzw/go-opentsdb/tsdb"
)

const (
	//environment variable SD_OTSDB_MAXMETRICS can be used to change max metrics to OpenTSDB
	SD_OTSDB_MAXMETRICS = 10
)

// StructPrettyPrint - JSON like
func StructPrettyPrint(s interface{}) string {
	bytesStruct, _ := json.MarshalIndent(s, "", "  ")
	return string(bytesStruct)
}

func openTSDB(config ConfigApp, buffer *bytes.Buffer) error {

	logCtx := log.WithFields(log.Fields{
		"in": "openTSDB",
	})

	maxMetrics := SD_OTSDB_MAXMETRICS
	if i, err := strconv.Atoi(os.Getenv("SD_OTSDB_MAXMETRICS")); err == nil && i > 0 {
		maxMetrics = i
	}

	if config.OpenTSDBAddress != "-" && config.OpenTSDBAddress != "" {
		datapoints := []tsdb.DataPoint{}
		TSDB := tsdb.TSDB{}
		server := tsdb.Server{}
		serverAdress := strings.Split(config.OpenTSDBAddress, ":")
		if len(serverAdress) != 2 {
			return fmt.Errorf("Incorrect OpenTSDB server address %v", serverAdress)
		}
		port, err := strconv.ParseUint(serverAdress[1], 10, 32)
		if err != nil {
			return err
		}
		server.Host = serverAdress[0]
		server.Port = uint(port)
		TSDB.Servers = append(TSDB.Servers, server)

		metrics := strings.Split(buffer.String(), "\n")
		metrics = removeEmptyLines(metrics)
		num := len(metrics)
		currentMetricsNum := 0
		for idx, mtr := range metrics {
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
					logCtx.Errorf("Only float/integer values allowed. Got: %s in line \"%s\". Error: %s", data[1], data, err)
					Stat.OtherErrorsInc()
					continue
				}
				value.Set(val)

				// parse timestamp
				err = timestamp.Parse(data[2])
				if err != nil {
					logCtx.Errorf("Timestamp expected. Got: %s in line \"%s\". Error: %s", data[2], data, err)
					Stat.OtherErrorsInc()
					continue
				}

				// parse metric/bucket
				metricName := data[0]
				err = metric.Set(metricName)
				if err != nil {
					logCtx.Errorf("Metric name expected. Got: %s in line \"%s\". Error: %s", data[0], data, err)
					Stat.OtherErrorsInc()
					continue
				}

				if len(data) == 4 {
					combinedTagsSlice := strings.Split(data[3], ",")
					if len(combinedTagsSlice) > 0 {
						for _, e := range combinedTagsSlice {
							strSlice := strings.Split(e, "=")
							if len(strSlice) == 2 {
								if strSlice[0] == "" || strSlice[1] == "" {
									logCtx.Errorf("Tag  expected. Got: %s in line \"%s\"", e, data)
									Stat.OtherErrorsInc()
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
				currentMetricsNum++
				// FIXME - heuristic that 10 is low enough to be accepted by OpenTSDB or env variable STATSDAEMON_MAXMETRICS
				if (currentMetricsNum%maxMetrics == 0) || idx == num-1 {
					out, err := TSDB.Put(datapoints)
					if len(out.Errors) > 0 || err != nil {
						sout := []string{}
						for _, elem := range out.Errors {
							sout = append(sout, elem.Error)
						}
						logCtx.Errorf("OpenTSDB.Put return: %s, error: %s", strings.Join(sout, "; "), err)
					}
					if err != nil {
						return err
					}
					datapoints = []tsdb.DataPoint{}
				}
			} else {
				logCtx.Errorf("Buffer format. Expected \"metric value timestamp\". Got \"%s\"", mtr)
				Stat.OtherErrorsInc()
			}

		}

		logCtx.Infof("sent %d stats to %s", currentMetricsNum, config.OpenTSDBAddress)

		return nil

	}
	return fmt.Errorf("No valid OpenTSDB address: %s", config.OpenTSDBAddress)

}
