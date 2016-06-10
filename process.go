package main

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sort"

	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
)

// packetHandler - process parsed packet and set data in
// global variables: tags, timers,gauges,counters,sets,keys
func packetHandler(s *Packet) {
	// New stat variable
	Stat.PointIncr()

	// global var tags
	// tags[s.Bucket] = s.Tags

	switch s.Modifier {
	// timer
	case "ms":
		_, ok := timers[s.Bucket]
		if !ok {
			var t Float64Slice
			timers[s.Bucket] = t
		}
		timers[s.Bucket] = append(timers[s.Bucket], s.Value.(float64))
		// gauge
	case "g":
		gaugeValue, _ := gauges[s.Bucket]

		gaugeData := s.Value.(GaugeData)
		if gaugeData.Relative {
			if gaugeData.Negative {
				// subtract checking for -ve numbers
				if gaugeData.Value > gaugeValue {
					gaugeValue = 0
				} else {
					gaugeValue -= gaugeData.Value
				}
			} else {
				// watch out for overflows
				if gaugeData.Value > (math.MaxFloat64 - gaugeValue) {
					gaugeValue = math.MaxFloat64
				} else {
					gaugeValue += gaugeData.Value
				}
			}
		} else {
			gaugeValue = gaugeData.Value
		}

		gauges[s.Bucket] = gaugeValue
		// counter
	case "c":
		_, ok := counters[s.Bucket]
		if !ok {
			counters[s.Bucket] = 0
		}
		// countInactivity[s.Bucket] = 0

		counters[s.Bucket] += int64(float64(s.Value.(int64)) * float64(1/s.Sampling))
		// set
	case "s":
		_, ok := sets[s.Bucket]
		if !ok {
			sets[s.Bucket] = make([]string, 0)
		}
		sets[s.Bucket] = append(sets[s.Bucket], s.Value.(string))
		// key/value
	case "kv":
		_, ok := keys[s.Bucket]
		if !ok {
			keys[s.Bucket] = make([]string, 0)
		}
		keys[s.Bucket] = append(keys[s.Bucket], s.Value.(string))
	}
}

func formatMetricOutput(bucket string, value interface{}, now int64, backend string) string {
	var ret, val string
	logCtx := log.WithFields(log.Fields{
		"in":  "formatMetricOutput",
		"ctx": "Format default output",
	})
	val = ""
	switch value.(type) {
	case string:
		val = value.(string)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		val = fmt.Sprintf("%d", value)
	case float32, float64:
		val = fmt.Sprintf("%f", value)
	default:
		logCtx.WithField("after", "default type").Errorf("Invalid type: %v", reflect.TypeOf(value))
		Stat.ErrorIncr()
	}

	cleanBucket, localTags, err := parseBucketAndTags(bucket)
	if err != nil {
		logCtx.WithField("after", "parseBucketAndTags").Errorf("%s", err)
		Stat.ErrorIncr()
	}

	setFirstGraphite := ""
	sepTags := ""
	if len(localTags) > 0 || len(Config.ExtraTagsHash) > 0 {
		setFirstGraphite = tfGraphiteFirstDelim
		if backend != "graphite" {
			sepTags = " "
		}
	}

	switch backend {
	case "external", "opentsdb", "dummy":
		ret = fmt.Sprintf("%s %s %d%s%s", cleanBucket, val, now, sepTags, normalizeTags(localTags, tfPretty))
	case "graphite":
		ret = fmt.Sprintf("%s%s%s%s %s %d", cleanBucket, setFirstGraphite, sepTags, normalizeTags(localTags, tfGraphite), val, now)
	default:
		ret = "UNKNOWN_BACKEND"
	}
	logCtx.WithField("after", "format line").Debugf("%s", ret)
	return ret
}

func processCounters(buffer *bytes.Buffer, now int64, reset bool, backend string, dbHandle *bolt.DB) int64 {
	// Normal behaviour is to reset couners after each send
	// "don't reset" was added for OpenTSDB and Grafana

	var (
		num                      int64
		err                      error
		startCounter, nowCounter MeasurePoint
	)
	logCtx := log.WithFields(log.Fields{
		"in":  "processCounters",
		"ctx": "calculate counters",
	})
	// continue sending zeros for counters for a short period of time even if we have no new data
	for bucket, value := range counters {

		if !reset {
			startCounter, err = readMeasurePoint(dbHandle, bucketName, bucket)
			if err != nil {
				logCtx.WithField("after", "readMeasurePoint").Errorf("%s", err)
				Stat.ErrorIncr()
			}
		} else {
			startCounter.Value = 0
			startCounter.When = now
		}

		nowCounter.Value = startCounter.Value + value
		nowCounter.When = now
		fmt.Fprintf(buffer, "%s\n", formatMetricOutput(bucket, nowCounter.Value, now, backend))
		delete(counters, bucket)
		// delete(tags, bucket)

		countInactivity[bucket] = 0

		if !reset {
			//  save counter to Bolt
			err = storeMeasurePoint(dbHandle, bucketName, bucket, nowCounter)
			if err != nil {
				logCtx.WithField("after", "storeMeasurePoint").Errorf("%s", err)
				Stat.ErrorIncr()
			}
		}
		num++
	}

	for bucket, purgeCount := range countInactivity {
		if purgeCount > 0 {
			// if not reset is is printed to buffer in the first loop (as it is not deleted)
			// untill there is some time of inactivity
			if !reset {
				startCounter, err = readMeasurePoint(dbHandle, bucketName, bucket)
				if err != nil {
					logCtx.WithField("after", "readMeasurePoint").Errorf("%s", err)
					Stat.ErrorIncr()
				}
			} else {
				startCounter.Value = 0
				startCounter.When = now
			}

			fmt.Fprintf(buffer, "%s\n", formatMetricOutput(bucket, startCounter.Value, now, backend))
			num++
		}
		countInactivity[bucket]++
		// remove counter from sending '0'
		if countInactivity[bucket] > Config.PersistCountKeys {
			delete(countInactivity, bucket)

		}
	}
	return num
}

func processGauges(buffer *bytes.Buffer, now int64, backend string) int64 {
	var num int64

	for bucket, gauge := range gauges {
		// FIXME MaxUint64 to MAxFloat64 ?????
		currentValue := gauge
		lastValue, hasLastValue := lastGaugeValue[bucket]

		var hasChanged bool

		if gauge != math.MaxUint64 {
			hasChanged = true
		}

		switch {
		case hasChanged:
			fmt.Fprintf(buffer, "%s\n", formatMetricOutput(bucket, currentValue, now, backend))
			// FIXME Memoryleak - never free lastGaugeValue & lastGaugeTags when a lot of unique bucket are used
			lastGaugeValue[bucket] = currentValue
			// lastGaugeTags[bucket] = tags[bucket]
			gauges[bucket] = math.MaxUint64
			// delete(tags, bucket)
			num++
		case hasLastValue && !hasChanged && !Config.DeleteGauges:
			fmt.Fprintf(buffer, "%s\n", formatMetricOutput(bucket, lastValue, now, backend))
			num++
		default:
			continue
		}
	}
	return num
}

func processSets(buffer *bytes.Buffer, now int64, backend string) int64 {
	num := int64(len(sets))
	for bucket, set := range sets {

		uniqueSet := map[string]bool{}
		for _, str := range set {
			uniqueSet[str] = true
		}

		fmt.Fprintf(buffer, "%s\n", formatMetricOutput(bucket, len(uniqueSet), now, backend))
		delete(sets, bucket)
		// delete(tags, bucket)
	}
	return num
}

func processKeyValue(buffer *bytes.Buffer, now int64, backend string) int64 {
	num := int64(len(keys))
	for bucket, values := range keys {
		uniqueKeyVal := map[string]bool{}
		// For each key in bucket `bucket`, process key, if key already in
		// uniqueKeyVal map, ignore it and move on, only one unique values
		// are possible, i.e. no duplicates.
		for _, value := range values {
			if _, ok := uniqueKeyVal[value]; ok {
				continue
			}
			uniqueKeyVal[value] = true
			fmt.Fprintf(buffer, "%s\n", formatMetricOutput(bucket, value, now, backend))
		}
		delete(keys, bucket)
		// delete(tags, bucket)
	}
	return num
}

func processTimers(buffer *bytes.Buffer, now int64, pctls Percentiles, backend string) int64 {
	// FIXME - chceck float64 conversion
	var num int64

	logCtx := log.WithFields(log.Fields{
		"in":  "processTimers",
		"ctx": "calculate timers",
	})
	for bucket, timer := range timers {
		bucketWithoutPostfix := bucket
		num++

		sort.Sort(timer)
		min := timer[0]
		max := timer[len(timer)-1]
		maxAtThreshold := max
		count := len(timer)

		sum := float64(0)
		for _, value := range timer {
			sum += value
		}
		mean := sum / float64(len(timer))

		// remove tags form bucketWithoutPostfix
		cleanBucket, localTags, err := parseBucketAndTags(bucketWithoutPostfix)
		if err != nil {
			logCtx.WithField("after", "parseBucketAndTags").Errorf("%s", err)
			Stat.ErrorIncr()
		}
		fullNormalizedTags := normalizeTags(addTags(localTags, Config.ExtraTagsHash), tfDefault)

		for _, pct := range pctls {
			if len(timer) > 1 {
				var abs float64
				if pct.Float >= 0 {
					abs = pct.Float
				} else {
					abs = 100 + pct.Float
				}
				// poor man's math.Round(x):
				// math.Floor(x + 0.5)
				indexOfPerc := int(math.Floor(((abs / 100.0) * float64(count)) + 0.5))
				if pct.Float >= 0 {
					indexOfPerc-- // index offset=0
				}
				maxAtThreshold = timer[indexOfPerc]
			}

			var tmpl string
			var pctstr string
			if pct.Float >= 0 {
				// tmpl = "%s.upper_%s%s %f %d\n"
				tmpl = "%s.upper_%s%s%s"
				pctstr = pct.Str
			} else {
				// tmpl = "%s.lower_%s%s %f %d\n"
				tmpl = "%s.lower_%s%s%s"
				pctstr = pct.Str[1:]
			}
			sep := ""
			if len(fullNormalizedTags) > 0 {
				sep = ".^"
			}

			fmt.Fprintf(buffer, "%s\n", formatMetricOutput(fmt.Sprintf(tmpl, cleanBucket, pctstr, sep, fullNormalizedTags), maxAtThreshold, now, backend))
		}

		sTags := fullNormalizedTags
		if len(sTags) > 0 {
			sTags = ".^" + sTags
		}

		fmt.Fprintf(buffer, "%s\n", formatMetricOutput(fmt.Sprintf("%s.mean%s", cleanBucket, sTags), mean, now, backend))
		fmt.Fprintf(buffer, "%s\n", formatMetricOutput(fmt.Sprintf("%s.upper%s", cleanBucket, sTags), max, now, backend))
		fmt.Fprintf(buffer, "%s\n", formatMetricOutput(fmt.Sprintf("%s.lower%s", cleanBucket, sTags), min, now, backend))
		fmt.Fprintf(buffer, "%s\n", formatMetricOutput(fmt.Sprintf("%s.count%s", cleanBucket, sTags), count, now, backend))
		delete(timers, bucket)
		// delete(localTags, bucket)
	}
	return num
}
