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

func openTSDB(openTSDBAddress string, buffer *bytes.Buffer, mtags map[string]string, debug bool) error {
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
				// FIXME - not working with k/v
				val, err := strconv.ParseFloat(data[1], 64)
				if err != nil {
					return err
				}
				value.Set(val)

				// parse timestamp
				err = timestamp.Parse(data[2])
				if err != nil {
					return err
				}
				metricAndTags := strings.SplitN(data[0], SEP_SPLIT, 1)
				fmt.Printf("DEBUG: data: (%v), metricAndTags: (%v)\n", data, metricAndTags)

				for k, v := range mtags[metricAndTags[0]] {
					tags.Set(k, v)
				}

				if metricAndTags[0] != data[0] {
					fmt.Printf("DEBUG (1): metric: %v\n", metricAndTags[0])
					err = metric.Set(metricAndTags[0])
					if err != nil {
						return err
					}
					//
					// for _, tagVal := range strings.Split(metricAndTags[1], SEP_SPLIT) {
					// 	arrTagVal := strings.Split(tagVal, "=")
					// 	fmt.Printf("DEBUG: tagval: %v\n", arrTagVal)
					// 	if len(arrTagVal) != 2 {
					// 		errmsg := fmt.Sprintf("Error: Incorrect metric format %v", arrTagVal)
					// 		return errors.New(errmsg)
					// 	}
					// 	tags.Set(arrTagVal[0], arrTagVal[1])
					// }

				} else {
					fmt.Printf("DEBUG (2): metric: %v\n", metricAndTags)

					metricAndTags := strings.Split(data[0], "._t_")
					if len(metricAndTags) != 2 {
						errmsg := fmt.Sprintf("Error: Incorrect metric format (._t_) %v", metricAndTags)
						return errors.New(errmsg)
					}
					err = metric.Set(metricAndTags[0])
					if err != nil {
						return err
					}
					arrTagVal := strings.Split(metricAndTags[1], ".")
					if len(arrTagVal) != 2 {
						errmsg := fmt.Sprintf("Error: Incorrect metric format (.) %v", arrTagVal)
						return errors.New(errmsg)
					}
					tags.Set(arrTagVal[0], arrTagVal[1])

					fmt.Printf("DEBUG: Tags (%v)", tags)

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
