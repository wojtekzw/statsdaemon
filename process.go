package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"reflect"
	"sort"
)

// packetHandler - process parsed packet and set data in
// global variables: tags, timers,gauges,counters,sets,keys
func packetHandler(s *Packet) {
	if *receiveCounter != "" {
		v, ok := counters[*receiveCounter]
		if !ok || v < 0 {
			counters[*receiveCounter] = 0
		}
		counters[*receiveCounter]++
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
		countInactivity[s.Bucket] = 0

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
	if len(tags) > 0 {
		sep = tfGraphiteFirstDelim
	}

	switch backend {
	case "external":
		ret = fmt.Sprintf("%s %s %d %s", cleanBucket, val, now, normalizeTags(tags, tfPretty))
	case "graphite":
		ret = fmt.Sprintf("%s%s%s %s %d", cleanBucket, sep, normalizeTags(tags, tfGraphite), val, now)

	case "opentsdb":
		ret = fmt.Sprintf("%s %s %d %s", cleanBucket, val, now, normalizeTags(tags, tfPretty))
	default:
		ret = ""
	}
	return ret
}

func processCounters(buffer *bytes.Buffer, now int64, reset bool, backend string) int64 {
	// Normal behaviour is to reset couners after each send
	// "don't reset" was added for OpenTSDB and Grafana

	var (
		num int64
	)

	// continue sending zeros for counters for a short period of time even if we have no new data
	for bucket, value := range counters {
		// fmt.Fprintf(buffer, "%s %d %d\n", bucket, value, now)
		fmt.Fprintf(buffer, "%s\n", formatMetricOutput(bucket, value, now, backend))
		if reset {
			delete(counters, bucket)
		}
		// countInactivity[bucket] is set to 0 when new packet arrives
		// in packetHandler
		num++
	}

	for bucket, purgeCount := range countInactivity {
		if purgeCount > 0 {
			// if not reset is is printed to buffer in the first loop (as it is not deleted)
			// untill there is some time of inactivity
			if reset {
				// fmt.Fprintf(buffer, "%s %d %d\n", bucket, 0, now)
				fmt.Fprintf(buffer, "%s\n", formatMetricOutput(bucket, 0, now, backend))
				num++
			}
		}
		countInactivity[bucket]++
		// remove counter from sending '0'
		if countInactivity[bucket] > *persistCountKeys {
			delete(countInactivity, bucket)
			// remove counter not deleted previously
			if !reset {
				delete(counters, bucket)
			}
			// remove tags associated with bucket
			delete(tags, bucket)
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
		case hasLastValue && !hasChanged && !*deleteGauges:
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
	for bucket, timer := range timers {
		bucketWithoutPostfix := bucket[:len(bucket)-len(*postfix)]
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
				tmpl = "%s.upper_%s.^%s%s"
				pctstr = pct.str
			} else {
				// tmpl = "%s.lower_%s%s %f %d\n"
				tmpl = "%s.lower_%s.^%s%s"
				pctstr = pct.str[1:]
			}
			// fmt.Fprintf(buffer, tmpl, bucketWithoutPostfix, pctstr, *postfix, maxAtThreshold, now)
			fmt.Fprintf(buffer, "%s\n", formatMetricOutput(fmt.Sprintf(tmpl, cleanBucket, pctstr, normalizeTags(tags, tfDefault), *postfix), maxAtThreshold, now, backend))
		}

		// fmt.Fprintf(buffer, "%s.mean%s %f %d\n", bucketWithoutPostfix, *postfix, mean, now)
		// fmt.Fprintf(buffer, "%s.upper%s %f %d\n", bucketWithoutPostfix, *postfix, max, now)
		// fmt.Fprintf(buffer, "%s.lower%s %f %d\n", bucketWithoutPostfix, *postfix, min, now)
		// fmt.Fprintf(buffer, "%s.count%s %d %d\n", bucketWithoutPostfix, *postfix, count, now)
		sTags := normalizeTags(tags, tfDefault)
		if len(sTags) > 0 {
			sTags = ".^" + sTags
		}

		fmt.Fprintf(buffer, "%s\n", formatMetricOutput(fmt.Sprintf("%s.mean%s%s", cleanBucket, sTags, *postfix), mean, now, backend))
		fmt.Fprintf(buffer, "%s\n", formatMetricOutput(fmt.Sprintf("%s.upper%s%s", cleanBucket, sTags, *postfix), max, now, backend))
		fmt.Fprintf(buffer, "%s\n", formatMetricOutput(fmt.Sprintf("%s.lower%s%s", cleanBucket, sTags, *postfix), min, now, backend))
		fmt.Fprintf(buffer, "%s\n", formatMetricOutput(fmt.Sprintf("%s.count%s%s", cleanBucket, sTags, *postfix), count, now, backend))
		delete(timers, bucket)
		delete(tags, bucket)
	}
	return num
}
