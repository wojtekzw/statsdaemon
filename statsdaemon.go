package main

import (
	"bytes"
	//	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	MAX_UNPROCESSED_PACKETS = 1000
	TCP_READ_SIZE           = 4096
	SEP_CHAR                = "^"
	SEP_SPLIT               = "." + SEP_CHAR
)

var signalchan chan os.Signal

type Packet struct {
	Bucket         string
	Value          interface{}
	SrcBucket      string
	GraphiteBucket string
	Tags           map[string]string
	Modifier       string
	Sampling       float32
}

type GaugeData struct {
	Relative bool
	Negative bool
	Value    float64
}

type Uint64Slice []uint64

func (s Uint64Slice) Len() int           { return len(s) }
func (s Uint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Uint64Slice) Less(i, j int) bool { return s[i] < s[j] }

type Float64Slice []float64

func (s Float64Slice) Len() int           { return len(s) }
func (s Float64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Float64Slice) Less(i, j int) bool { return s[i] < s[j] }

type Percentiles []*Percentile
type Percentile struct {
	float float64
	str   string
}

func (a *Percentiles) Set(s string) error {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*a = append(*a, &Percentile{f, strings.Replace(s, ".", "_", -1)})
	return nil
}
func (p *Percentile) String() string {
	return p.str
}
func (a *Percentiles) String() string {
	return fmt.Sprintf("%v", *a)
}

func sanitizeBucket(bucket string) string {
	b := make([]byte, len(bucket))
	var bl int

	for i := 0; i < len(bucket); i++ {
		c := bucket[i]
		switch {
		case (c >= byte('a') && c <= byte('z')) || (c >= byte('A') && c <= byte('Z')) || (c >= byte('0') && c <= byte('9')) || c == byte('-') || c == byte('.') || c == byte('_') || c == byte('='):
			b[bl] = c
			bl++
		case c == byte(' '):
			b[bl] = byte('_')
			bl++
		case c == byte('/'):
			b[bl] = byte('-')
			bl++
		}
	}
	return string(b[:bl])
}

var (
	serviceAddress    = flag.String("address", ":8125", "UDP service address")
	tcpServiceAddress = flag.String("tcpaddr", "", "TCP service address, if set")
	maxUdpPacketSize  = flag.Int64("max-udp-packet-size", 1472, "Maximum UDP packet size")
	backendType       = flag.String("backend-type", "external", "Backend to use: graphite, opentsdb, external")
	postFlushCmd      = flag.String("post-flush-cmd", "stdout", "Command to run on each flush")
	graphiteAddress   = flag.String("graphite", "127.0.0.1:2003", "Graphite service address (or - to disable)")
	openTSDBAddress   = flag.String("opentsdb", "127.0.0.1:4242", "openTSDB service address (or - to disable)")
	flushInterval     = flag.Int64("flush-interval", 10, "Flush interval (seconds)")
	debug             = flag.Bool("debug", false, "print statistics sent to backend")
	showVersion       = flag.Bool("version", false, "print version string")
	deleteGauges      = flag.Bool("delete-gauges", true, "don't send values to graphite for inactive gauges, as opposed to sending the previous value")
	resetCounters     = flag.Bool("reset-counters", true, "reset counters after sending value to backend or leave current value (eg. for OpenTSDB & Grafana)")
	persistCountKeys  = flag.Int64("persist-count-keys", 60, "number of flush-intervals to persist count keys")
	receiveCounter    = flag.String("receive-counter", "", "Metric name for total metrics received per interval")
	percentThreshold  = Percentiles{}
	prefix            = flag.String("prefix", "", "Prefix for all stats")
	postfix           = flag.String("postfix", "", "Postfix for all stats")
)

func init() {
	flag.Var(&percentThreshold, "percent-threshold",
		"percentile calculation for timers (0-100, may be given multiple times)")
}

var (
	In              = make(chan *Packet, MAX_UNPROCESSED_PACKETS)
	counters        = make(map[string]int64)
	gauges          = make(map[string]float64)
	lastGaugeValue  = make(map[string]float64)
	lastGaugeTags   = make(map[string]map[string]string)
	timers          = make(map[string]Float64Slice)
	countInactivity = make(map[string]int64)
	sets            = make(map[string][]string)
	keys            = make(map[string][]string)
	tags            = make(map[string]map[string]string)
)

func monitor() {
	period := time.Duration(*flushInterval) * time.Second
	ticker := time.NewTicker(period)
	for {
		select {
		case sig := <-signalchan:
			fmt.Printf("!! Caught signal %v... shutting down\n", sig)
			if err := submit(time.Now().Add(period), *backendType); err != nil {
				log.Printf("ERROR: submit %s", err)
			}
			return
		case <-ticker.C:
			if err := submit(time.Now().Add(period), *backendType); err != nil {
				log.Printf("ERROR: submit %s", err)
			}
		case s := <-In:
			packetHandler(s)
		}
	}
}

// packetHandler - process parsed packet and set data in
// global variables: tags, timers,gauges,counters,sets,keys
func packetHandler(s *Packet) {
	if *receiveCounter != "" {
		v, ok := counters[*receiveCounter]
		if !ok || v < 0 {
			counters[*receiveCounter] = 0
		}
		counters[*receiveCounter] += 1
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

func submit(deadline time.Time, backend string) error {
	var buffer bytes.Buffer
	var num int64
	now := time.Now().Unix()

	num += processCounters(&buffer, now, *resetCounters)
	num += processGauges(&buffer, now)
	num += processTimers(&buffer, now, percentThreshold)
	num += processSets(&buffer, now)
	num += processKeyValue(&buffer, now)

	if num == 0 {
		return nil
	}

	if *debug {
		for _, line := range bytes.Split(buffer.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			log.Printf("DEBUG: %s", line)
		}
	}
	// send stats to backend
	switch backend {
	case "external":
		log.Printf("DEBUG: external [%s]\n", fixNewLine(buffer.String()))

		if *postFlushCmd != "stdout" {
			err := sendDataExtCmd(*postFlushCmd, &buffer)
			if err != nil {
				log.Printf(err.Error())
			}
			log.Printf("sent %d stats to external command", num)
		} else {
			if err := sendDataStdout(&buffer); err != nil {
				log.Printf(err.Error())
			}
		}

	case "graphite":
		log.Printf("DEBUG: graphite [%s]\n", fixNewLine(buffer.String()))

		client, err := net.Dial("tcp", *graphiteAddress)
		if err != nil {
			return fmt.Errorf("dialing %s failed - %s", *graphiteAddress, err)
		}
		defer client.Close()

		err = client.SetDeadline(deadline)
		if err != nil {
			return err
		}

		_, err = client.Write(buffer.Bytes())
		if err != nil {
			return fmt.Errorf("failed to write stats to graphite: %s", err)
		}
		log.Printf("wrote %d stats to graphite(%s)", num, *graphiteAddress)

	case "opentsdb":
		if *debug {
			log.Printf("DEBUG: opentsdb [%s]\n", fixNewLine(buffer.String()))
		}

		err := openTSDB(*openTSDBAddress, &buffer, tags, *debug)
		if err != nil {
			log.Printf("Error writing to OpenTSDB: %v\n", err)
		}

	default:
		log.Printf("%v", fmt.Errorf("Invalid backend %s. Exiting...\n", backend))
		os.Exit(1)
	}

	return nil
}

func processCounters(buffer *bytes.Buffer, now int64, reset bool) int64 {
	// Normal behaviour is to reset couners after each send
	// "don't reset" was added for OpenTSDB and Grafana

	var (
		num int64
	)

	// continue sending zeros for counters for a short period of time even if we have no new data
	for bucket, value := range counters {
		fmt.Fprintf(buffer, "%s %d %d\n", bucket, value, now)
		if reset {
			delete(counters, bucket)
		}
		// _, ok := countInactivity[bucket]
		// // if not in inactive counters then set to 0 else let it grow
		// if !ok {
		// 	// set counter as inactive
		// 	countInactivity[bucket] = 0
		// }
		num++
	}

	for bucket, purgeCount := range countInactivity {
		if purgeCount > 0 {
			// if not reset is is printed to buffer in the first loop (as it is not deleted)
			// untill there is some time of inactivity
			if reset {
				fmt.Fprintf(buffer, "%s %d %d\n", bucket, 0, now)
				num++
			}
		}
		countInactivity[bucket] += 1
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

func processGauges(buffer *bytes.Buffer, now int64) int64 {
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
			fmt.Fprintf(buffer, "%s %f %d\n", bucket, currentValue, now)
			// FIXME Memoryleak - never free lastGaugeValue & lastGaugeTags when a lot of unique bucket are used
			lastGaugeValue[bucket] = currentValue
			lastGaugeTags[bucket] = tags[bucket]
			gauges[bucket] = math.MaxUint64
			delete(tags, bucket)
			num++
		case hasLastValue && !hasChanged && !*deleteGauges:
			fmt.Fprintf(buffer, "%s %f %d\n", bucket, lastValue, now)
			num++
		default:
			continue
		}
	}
	return num
}

func processSets(buffer *bytes.Buffer, now int64) int64 {
	num := int64(len(sets))
	for bucket, set := range sets {

		uniqueSet := map[string]bool{}
		for _, str := range set {
			uniqueSet[str] = true
		}

		fmt.Fprintf(buffer, "%s %d %d\n", bucket, len(uniqueSet), now)
		delete(sets, bucket)
		delete(tags, bucket)
	}
	return num
}

func processKeyValue(buffer *bytes.Buffer, now int64) int64 {
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
			fmt.Fprintf(buffer, "%s %s %d\n", bucket, value, now)
		}
		delete(keys, bucket)
		delete(tags, bucket)
	}
	return num
}

func processTimers(buffer *bytes.Buffer, now int64, pctls Percentiles) int64 {
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
					indexOfPerc -= 1 // index offset=0
				}
				maxAtThreshold = timer[indexOfPerc]
			}

			var tmpl string
			var pctstr string
			if pct.float >= 0 {
				tmpl = "%s.upper_%s%s %f %d\n"
				pctstr = pct.str
			} else {
				tmpl = "%s.lower_%s%s %f %d\n"
				pctstr = pct.str[1:]
			}
			fmt.Fprintf(buffer, tmpl, bucketWithoutPostfix, pctstr, *postfix, maxAtThreshold, now)

		}

		fmt.Fprintf(buffer, "%s.mean%s %f %d\n", bucketWithoutPostfix, *postfix, mean, now)
		fmt.Fprintf(buffer, "%s.upper%s %f %d\n", bucketWithoutPostfix, *postfix, max, now)
		fmt.Fprintf(buffer, "%s.lower%s %f %d\n", bucketWithoutPostfix, *postfix, min, now)
		fmt.Fprintf(buffer, "%s.count%s %d %d\n", bucketWithoutPostfix, *postfix, count, now)

		delete(timers, bucket)
		delete(tags, bucket)
	}
	return num
}

type MsgParser struct {
	reader       io.Reader
	buffer       []byte
	partialReads bool
	done         bool
}

func NewParser(reader io.Reader, partialReads bool) *MsgParser {
	return &MsgParser{reader, []byte{}, partialReads, false}
}

func (mp *MsgParser) Next() (*Packet, bool) {
	buf := mp.buffer

	for {
		line, rest := mp.lineFrom(buf)

		if line != nil {
			mp.buffer = rest
			return parseLine(line), true
		}

		if mp.done {
			return parseLine(rest), false
		}

		idx := len(buf)
		end := idx
		if mp.partialReads {
			end += TCP_READ_SIZE
		} else {
			end += int(*maxUdpPacketSize)
		}
		if cap(buf) >= end {
			buf = buf[:end]
		} else {
			tmp := buf
			buf = make([]byte, end)
			copy(buf, tmp)
		}

		n, err := mp.reader.Read(buf[idx:])
		buf = buf[:idx+n]
		if err != nil {
			if err != io.EOF {
				log.Printf("ERROR: %s", err)
			}

			mp.done = true

			line, rest = mp.lineFrom(buf)
			if line != nil {
				mp.buffer = rest
				return parseLine(line), len(rest) > 0
			}

			if len(rest) > 0 {
				return parseLine(rest), false
			}

			return nil, false
		}
	}
}

func (mp *MsgParser) lineFrom(input []byte) ([]byte, []byte) {
	split := bytes.SplitAfterN(input, []byte("\n"), 2)
	if len(split) == 2 {
		return split[0][:len(split[0])-1], split[1]
	}

	if !mp.partialReads {
		if len(input) == 0 {
			input = nil
		}
		return input, []byte{}
	}

	if bytes.HasSuffix(input, []byte("\n")) {
		return input[:len(input)-1], []byte{}
	}

	return nil, input
}

func parseLine(line []byte) *Packet {

	tags := make(map[string]string)

	if *debug {
		log.Printf("DEBUG: Input packet line: %s", string(line))
	}

	split := bytes.SplitN(line, []byte{'|'}, 3)
	if len(split) < 2 {
		logParseFail(line)
		return nil
	}

	keyval := split[0]
	typeCode := string(split[1]) // expected c, g, s, ms, kv

	sampling := float32(1)
	if strings.HasPrefix(typeCode, "c") || strings.HasPrefix(typeCode, "ms") {
		if len(split) == 3 && len(split[2]) > 0 && split[2][0] == '@' {
			f64, err := strconv.ParseFloat(string(split[2][1:]), 32)
			if err != nil {
				log.Printf(
					"ERROR: failed to ParseFloat %s - %s",
					string(split[2][1:]),
					err,
				)
				return nil
			}
			sampling = float32(f64)
		}
	}

	split = bytes.SplitN(keyval, []byte{':'}, 2)
	if len(split) < 2 {
		logParseFail(line)
		return nil
	}
	// raw bucket name from line
	name := string(split[0])
	val := split[1]
	if len(val) == 0 {
		logParseFail(line)
		return nil
	}

	var (
		err    error
		value  interface{}
		bucket string
	)

	switch typeCode {
	case "c":
		value, err = strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			log.Printf("ERROR: failed to ParseInt %s - %s", string(val), err)
			return nil
		}
	case "g":
		var rel, neg bool
		var s string

		switch val[0] {
		case '+':
			rel = true
			neg = false
			s = string(val[1:])
		case '-':
			rel = true
			neg = true
			s = string(val[1:])
		default:
			rel = false
			neg = false
			s = string(val)
		}

		value, err = strconv.ParseFloat(s, 64)
		if err != nil {
			log.Printf("ERROR: Gauge - failed to ParseFloat %s - %s", string(val), err)
			return nil
		}

		value = GaugeData{rel, neg, value.(float64)}
	case "s":
		value = string(val)
	case "ms":
		value, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			log.Printf("ERROR: Timer - failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	case "kv":
		value = string(val) // Key/value should not need transformation
	default:
		log.Printf("ERROR: unrecognized type code %q", typeCode)
		return nil
	}
	// parse tags from bucket name
	bucket, tags, err = parseBucketAndTags(string(name))

	// bucket is set to a name without tags (tagsSlice[0])
	// graphiteBucket is set to a name striped out of not allowed chars
	bucket = sanitizeBucket(*prefix + bucket + *postfix)
	graphiteBucket := sanitizeBucket(*prefix + string(name) + *postfix)
	return &Packet{
		Bucket:         bucket,
		Value:          value,
		SrcBucket:      string(name),
		GraphiteBucket: graphiteBucket,
		Tags:           tags,
		Modifier:       typeCode,
		Sampling:       sampling,
	}
}

func parseBucketAndTags(name string) (string, map[string]string, error) {
	// split name in format
	// measure.name.^tag1=val1.^tag2=val2
	// this function can be extended for new "combined" formats

	tags := make(map[string]string)

	tagsSlice := strings.Split(string(name), SEP_SPLIT)
	if len(tagsSlice) == 0 || tagsSlice[0] == "" {
		return "", nil, fmt.Errorf("Format error: Invalid bucket name in \"%s\"", name)
	}
	for _, e := range tagsSlice[1:] {
		tagAndVal := strings.Split(e, "=")
		if len(tagAndVal) != 2 || tagAndVal[0] == "" || tagAndVal[1] == "" {
			log.Printf("Error: invalid tag format %v", e)
		} else {
			tags[tagAndVal[0]] = tagAndVal[1]
		}
	}
	return tagsSlice[0], tags, nil
}
func logParseFail(line []byte) {
	if *debug {
		log.Printf("ERROR: failed to parse line: %q\n", string(line))
	}
}

func parseTo(conn io.ReadCloser, partialReads bool, out chan<- *Packet) {
	defer conn.Close()

	parser := NewParser(conn, partialReads)
	for {
		p, more := parser.Next()
		if p != nil {
			out <- p
		}

		if !more {
			break
		}
	}
}

func udpListener() {
	address, _ := net.ResolveUDPAddr("udp", *serviceAddress)
	log.Printf("listening on %s", address)
	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenUDP - %s", err)
	}

	parseTo(listener, false, In)
}

func tcpListener() {
	address, _ := net.ResolveTCPAddr("tcp", *tcpServiceAddress)
	log.Printf("listening on %s", address)
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenTCP - %s", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Fatalf("ERROR: AcceptTCP - %s", err)
		}
		go parseTo(conn, true, In)
	}
}

func validateFlags() error {
	// FIXME  check all params/flags
	if *backendType != "external" && *backendType != "graphite" && *backendType != "opentsdb" {
		return fmt.Errorf("Parameter error: Invalid backend-type: %s\n", *backendType)
	}

	if *graphiteAddress == "-" && *backendType == "graphite" {
		return fmt.Errorf("Parameter error: Graphite backend selected and no graphite server address\n")
	}

	if *openTSDBAddress == "-" && *backendType == "opentsdb" {
		return fmt.Errorf("Parameter error: OpenTSDB backend selected and no OpenTSDB server address\n")
	}
	return nil
}

func removeEmptyLines(lines []string) []string {
	var outLines []string

	for _, v := range lines {
		if len(v) > 0 {
			outLines = append(outLines, v)
		}
	}
	return outLines
}

func fixNewLine(s string) string {
	return strings.Replace(s, "\n", "\\n", -1)
}

func tagsToString(t map[string]string) string {
	out := ""
	keys := sort.StringSlice{}
	for k := range t {
		keys = append(keys, k)
	}
	keys.Sort()

	for _, k := range keys {
		out = out + fmt.Sprintf(", %s=%s", k, t[k])
	}
	return out
}

// func getBytes(key interface{}) (bytes.Buffer, error) {
// 	var buf = bytes.Buffer{}
//
// 	enc := gob.NewEncoder(&buf)
// 	err := enc.Encode(key)
// 	if err != nil {
// 		return buf, err
// 	}
// 	return buf, nil
// }

// func formatOutLine(buffer bytes.Buffer, allTags map[string]map[string]string) bytes.Buffer {
// 	out := ""
// 	metrics := strings.Split(buffer.String(), "\n")
// 	metrics = removeEmptyLines(metrics)
// 	for _, mtr := range metrics {
// 		data := strings.Split(mtr, " ")
// 		out = mtr
// 		if len(data) == 3 {
// 			out = out + " " + tagsToString(allTags[data[0]])
// 		}
// 		out = out + "\n"
// 	}
// 	fmt.Printf("DEBUG: TAGS %s", out)
// 	buf, err := getBytes(out)
// 	if err != nil {
// 		return bytes.Buffer{}
// 	}
// 	return buf
// }

func main() {
	flag.Parse()
	err := validateFlags()
	if err != nil {
		fmt.Printf("\n%s\n", err)
		flag.Usage()
		os.Exit(1)
	}

	if *showVersion {
		fmt.Printf("statsdaemon v%s (built w/%s)\n", VERSION, runtime.Version())
		os.Exit(0)
	}

	signalchan = make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGTERM)

	go udpListener()
	if *tcpServiceAddress != "" {
		go tcpListener()
	}
	monitor()
}
