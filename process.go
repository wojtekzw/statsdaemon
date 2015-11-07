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
	if Config.ReceiveCounterWithTags != "" {
		v, ok := counters[Config.ReceiveCounterWithTags]
		if !ok || v < 0 {
			counters[Config.ReceiveCounterWithTags] = 0
		}
		counters[Config.ReceiveCounterWithTags]++
	}

	// global var tags
	tags[s.Bucket] = s.Tags

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
	val = ""
	switch value.(type) {
	case string:
		val = value.(string)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		val = fmt.Sprintf("%d", value)
	case float32, float64:
		val = fmt.Sprintf("%f", value)
	default:
		log.Printf("Error - formatMetricOutput: Invalid type: %v", reflect.TypeOf(value))
	}
	cleanBucket, tags, err := parseBucketAndTags(bucket)
	if err != nil {
		log.Printf("Error - formatMetricOutput: %v", err)
	}

	sep := ""
	sepTags := ""
	if len(tags) > 0 || len(Config.ExtraTagsHash) > 0 {
		sep = tfGraphiteFirstDelim
		sepTags = " "
	}

	switch backend {
	case "external":
		ret = fmt.Sprintf("%s %s %d%s%s", cleanBucket, val, now, sepTags, normalizeTags(tags, tfPretty))
	case "graphite":
		ret = fmt.Sprintf("%s%s%s %s%s%d", cleanBucket, sep, sepTags, normalizeTags(tags, tfGraphite), val, now)

	case "opentsdb":
		ret = fmt.Sprintf("%s %s %d%s%s", cleanBucket, val, now, sepTags, normalizeTags(tags, tfPretty))
	default:
		ret = ""
	}
	fmt.Printf("DEBUG-BACKEND:%s\n", ret)
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

	// continue sending zeros for counters for a short period of time even if we have no new data
	for bucket, value := range counters {

		if !reset {
			startCounter, err = readMeasurePoint(dbHandle, bucketName, bucket)
			if err != nil {
				log.Printf("%s", err)
			}
		} else {
			startCounter.Value = 0
			startCounter.When = now
		}

		nowCounter.Value = startCounter.Value + value
		nowCounter.When = now
		fmt.Fprintf(buffer, "%s\n", formatMetricOutput(bucket, nowCounter.Value, now, backend))
		delete(counters, bucket)
		delete(tags, bucket)

		countInactivity[bucket] = 0

		if !reset {
			//  save counter to Bolt
			err = storeMeasurePoint(dbHandle, bucketName, bucket, nowCounter)
			if err != nil {
				log.Printf("%s", err)
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
					log.Printf("%s", err)
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
		// not used
		// lastTags := lastGaugeTags[bucket]
		var hasChanged bool

		if gauge != math.MaxUint64 {
			hasChanged = true
		}

		switch {
		case hasChanged:
			// fmt.Fprintf(buffer, "%s %f %d\n", bucket, currentValue, now)
			fmt.Fprintf(buffer, "%s\n", formatMetricOutput(bucket, currentValue, now, backend))
			// FIXME Memoryleak - never free lastGaugeValue & lastGaugeTags when a lot of unique bucket are used
			lastGaugeValue[bucket] = currentValue
			lastGaugeTags[bucket] = tags[bucket]
			gauges[bucket] = math.MaxUint64
			delete(tags, bucket)
			num++
		case hasLastValue && !hasChanged && !Config.DeleteGauges:
			// fmt.Fprintf(buffer, "%s %f %d\n", bucket, lastValue, now)
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

		// fmt.Fprintf(buffer, "%s %d %d\n", bucket, len(uniqueSet), now)
		fmt.Fprintf(buffer, "%s\n", formatMetricOutput(bucket, len(uniqueSet), now, backend))
		delete(sets, bucket)
		delete(tags, bucket)
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
			// fmt.Fprintf(buffer, "%s %s %d\n", bucket, value, now)
			fmt.Fprintf(buffer, "%s\n", formatMetricOutput(bucket, value, now, backend))
		}
		delete(keys, bucket)
		delete(tags, bucket)
	}
	return num
}

func processTimers(buffer *bytes.Buffer, now int64, pctls Percentiles, backend string) int64 {
	// FIXME - chceck float64 conversion
	var num int64

	// strExtraTags := normalizeTags(Config.ExtraTagsHash, tfDefault)
	// lenExtraTags := len(strExtraTags)
	for bucket, timer := range timers {
		// log.Printf("TIMERS: bucket=(%s) timer=(%s) lenExtraTags=(%d) strExtraTags=(%s)\n", bucket, timer, lenExtraTags, strExtraTags)
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
		cleanBucket, tags, err := parseBucketAndTags(bucketWithoutPostfix)
		if err != nil {
			log.Printf("Error parse - processTimers: %v", err)
		}
		fullNormalizedTags := normalizeTags(addTags(tags, Config.ExtraTagsHash), tfDefault)

		for _, pct := range pctls {
			if len(timer) > 1 {
				var abs float64
				if pct.float >= 0 {
					abs = pct.float
				} else {
					abs = 100 + pct.float
				}
				// poor man's math.Round(x):
				// math.Floor(x + 0.5)
				indexOfPerc := int(math.Floor(((abs / 100.0) * float64(count)) + 0.5))
				if pct.float >= 0 {
					indexOfPerc-- // index offset=0
				}
				maxAtThreshold = timer[indexOfPerc]
			}

			var tmpl string
			var pctstr string
			if pct.float >= 0 {
				// tmpl = "%s.upper_%s%s %f %d\n"
				tmpl = "%s.upper_%s%s%s"
				pctstr = pct.str
			} else {
				// tmpl = "%s.lower_%s%s %f %d\n"
				tmpl = "%s.lower_%s%s%s"
				pctstr = pct.str[1:]
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
		delete(tags, bucket)
	}
	return num
}
