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

func openTSDB(openTSDBAddress string, buffer *bytes.Buffer, debug bool) error {
	if openTSDBAddress != "-" {
		datapoints := []tsdb.DataPoint{}
		TSDB := tsdb.TSDB{}
		server := tsdb.Server{}
		serverAdress := strings.Split(openTSDBAddress, ":")
		if len(serverAdress) != 2 {
			errmsg := fmt.Sprintf("Error: Incorrect openTSDB server address")
			return errors.New(errmsg)
		}
		port, err := strconv.ParseUint(serverAdress[1], 10, 32)
		if err != nil {
			return err
		}
		server.Host = serverAdress[0]
		server.Port = uint(port)
		metrics := strings.Split(buffer.String(), "\n")
		log.Printf("ERROR: %v", metrics)
		num := len(metrics)
		for _, mtr := range metrics {
			data := strings.Split(mtr, " ")
			if len(data) == 3 {
				metric := tsdb.Metric{}
				value := tsdb.Value{}
				tags := tsdb.Tags{}
				timestamp := tsdb.Time{}
				datapoint := tsdb.DataPoint{}
				val, err := strconv.ParseFloat(data[1], 64)
				if err != nil {
					return err
				}
				value.Set(val)
				err = timestamp.Parse(data[2])
				if err != nil {
					return err
				}
				metricAndTags := strings.Split(data[0], "?")
				if metricAndTags[0] != data[0] {
					err = metric.Set(metricAndTags[0])
					if err != nil {
						return err
					}
					for _, tagVal := range strings.Split(metricAndTags[1], "&") {
						arrTagVal := strings.Split(tagVal, "=")
						if len(arrTagVal) != 2 {
							errmsg := fmt.Sprintf("Error: Incorrect metric format")
							return errors.New(errmsg)
						}
						tags.Set(arrTagVal[0], arrTagVal[1])
					}
				} else {
					metricAndTags := strings.Split(data[0], "._t_")
					if len(metricAndTags) != 2 {
						errmsg := fmt.Sprintf("Error: Incorrect metric format")
						return errors.New(errmsg)
					}
					err = metric.Set(metricAndTags[0])
					if err != nil {
						return err
					}
					arrTagVal := strings.Split(metricAndTags[1], ".")
					if len(arrTagVal) != 2 {
						errmsg := fmt.Sprintf("Error: Incorrect metric format")
						return errors.New(errmsg)
					}
					tags.Set(arrTagVal[0], arrTagVal[1])

					datapoint.Value = &value
					datapoint.Metric = &metric
					datapoint.Tags = &tags
					datapoint.Timestamp = &timestamp
					datapoints = append(datapoints, datapoint)
				}

			}

			TSDB.Servers = append(TSDB.Servers, server)
			_, err = TSDB.Put(datapoints)
			if err != nil {
				return err
			}
			log.Printf("sent %d stats to %s", num, openTSDBAddress)

		}
	}
	return nil
}
