// opentsdb.go
package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/mapmyfitness/go-opentsdb/tsdb"
)

func openTSDB(openTSDBAddress string, buffer *bytes.Buffer, mtags map[string]map[string]string, debug bool) error {

	if openTSDBAddress != "-" {
		datapoints := []tsdb.DataPoint{}
		TSDB := tsdb.TSDB{}
		server := tsdb.Server{}
		serverAdress := strings.Split(openTSDBAddress, ":")
		if len(serverAdress) != 2 {
			errmsg := fmt.Sprintf("Error: Incorrect openTSDB server address %v", serverAdress)
			return errors.New(errmsg)
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
			data := strings.Split(mtr, " ")
			if len(data) == 3 {
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
					log.Printf("Format error: Only float/integer values allowed. Got: %s in line \"%s\". Error: %s", data[1], data, err)
					continue
				}
				value.Set(val)

				// parse timestamp
				err = timestamp.Parse(data[2])
				if err != nil {
					log.Printf("Format error: Timestamp expected. Got: %s in line \"%s\". Error: %s", data[2], data, err)
					continue
				}

				// parse metric/bucket
				metricName := data[0]
				err = metric.Set(metricName)
				if err != nil {
					log.Printf("Format error: Metric name expected. Got: %s in line \"%s\". Error: %s", data[0], data, err)
					continue
				}

				for k, v := range mtags[metricName] {
					tags.Set(k, v)
				}

				datapoint.Value = &value
				datapoint.Metric = &metric
				datapoint.Tags = &tags
				datapoint.Timestamp = &timestamp
				datapoints = append(datapoints, datapoint)
			} else {
				log.Printf("Format error: Buffer format. Expected \"metric value timestamp\". Got \"%s\"", mtr)
			}

		}

		TSDB.Servers = append(TSDB.Servers, server)
		_, err = TSDB.Put(datapoints)
		if err != nil {
			return err
		}
		log.Printf("sent %d stats to %s", num, openTSDBAddress)

		return nil

	}

	return nil
}
