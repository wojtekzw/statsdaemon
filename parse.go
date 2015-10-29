package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
)

// Constants for tag separator encoded in bucket name
const (
	SEP_CHAR  = "^"
	SEP_SPLIT = "." + SEP_CHAR
)

// GaugeData - struct for gauges :)
type GaugeData struct {
	Relative bool
	Negative bool
	Value    float64
}

// Packet - meter definition, read from statsd format
type Packet struct {
	Bucket         string
	Value          interface{}
	SrcBucket      string
	GraphiteBucket string
	Tags           map[string]string
	Modifier       string
	Sampling       float32
}

// MsgParser - struct for reading data from UDP/TCP packet
type MsgParser struct {
	reader       io.Reader
	buffer       []byte
	partialReads bool
	done         bool
}

// NewParser - for UDP/TCP packet
func NewParser(reader io.Reader, partialReads bool) *MsgParser {
	return &MsgParser{reader, []byte{}, partialReads, false}
}

// Next - for reading whole meter data from packet
// return *Packet parsed from raw data
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