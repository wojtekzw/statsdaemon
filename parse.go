package main

// Functions connected with parsing data form original statsd format

import (
	"bytes"
	"io"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
)

// GaugeData - struct for gauges :)
type GaugeData struct {
	Relative bool
	Negative bool
	Value    float64
}

// Packet - meter definition, read from statsd format
type Packet struct {
	Bucket      string
	Value       interface{}
	SrcBucket   string
	CleanBucket string
	Tags        map[string]string
	Modifier    string
	Sampling    float32
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

	logCtx := log.WithFields(log.Fields{
		"in":  "MsgParser Next",
		"ctx": "Parse packet",
	})
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
			end += tcpReadSize
		} else {
			end += int(Config.MaxUDPPacketSize)
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
				logCtx.WithField("after", "Read").Errorf("%s", err)
				Stat.ErrorIncr()
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

	logCtx := log.WithFields(log.Fields{
		"in":  "parseLine",
		"ctx": "Parse packet",
	})

	tags := make(map[string]string)

	logCtx.WithField("after", "parseLine").Debugf("Input packet line: %s", string(line))

	split := bytes.SplitN(line, []byte{'|'}, 3)
	if len(split) < 2 {
		logCtx.WithField("after", "parseLine").Errorf("Failed to parse line: %s", line)
		Stat.ErrorIncr()
		return nil
	}

	keyval := split[0]
	typeCode := string(split[1]) // expected c, g, s, ms, kv

	sampling := float32(1)
	if strings.HasPrefix(typeCode, "c") || strings.HasPrefix(typeCode, "ms") {
		if len(split) == 3 && len(split[2]) > 0 && split[2][0] == '@' {
			f64, err := strconv.ParseFloat(string(split[2][1:]), 32)
			if err != nil {
				logCtx.WithField("after", "ParseFloat").Errorf("Failed to ParseFloat %s - %s", string(split[2][1:]), err)
				Stat.ErrorIncr()
				return nil
			}
			sampling = float32(f64)
		}
	}

	split = bytes.SplitN(keyval, []byte{':'}, 2)
	if len(split) < 2 {
		logCtx.WithField("after", "parseLine").Errorf("Failed to parse line: %s", line)
		Stat.ErrorIncr()
		return nil
	}
	// raw bucket name from line
	name := string(split[0])
	val := split[1]
	if len(val) == 0 {
		logCtx.WithField("after", "parseLine").Errorf("Failed to parse line: %s", line)
		Stat.ErrorIncr()
		return nil
	}

	var (
		err         error
		value       interface{}
		bucket      string
		cleanBucket string
	)

	switch typeCode {
	case "c":
		value, err = strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			logCtx.WithField("after", "Counter - ParseInt").Errorf("Failed to ParseInt %s - %s", string(val), err)
			Stat.ErrorIncr()
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
			logCtx.WithField("after", "Gauge - ParseFloat").Errorf("Failed to ParseFloat %s - %s", string(val), err)
			Stat.ErrorIncr()
			return nil
		}

		value = GaugeData{rel, neg, value.(float64)}
	case "s":
		value = string(val)
	case "ms":
		value, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			logCtx.WithField("after", "Timer - ParseFloat").Errorf("Failed to ParseFloat %s - %s", string(val), err)
			Stat.ErrorIncr()
			return nil
		}
	case "kv":
		value = string(val) // Key/value should not need transformation
	default:
		logCtx.WithField("after", "default").Errorf("Unrecognized type code %q", typeCode)
		Stat.ErrorIncr()
		return nil
	}

	// parse tags from bucket name
	cleanBucket, tags, err = parseBucketAndTags(string(name))
	if err != nil {
		logCtx.WithField("after", "parseBucketAndTags").Errorf("Problem parsing %s (clean version %s): %v\n", string(name), cleanBucket, err)
		Stat.ErrorIncr()
		return nil
	}

	// bucket is set to a name WITH tags
	firstDelim := ""
	if len(tags) > 0 || len(Config.ExtraTagsHash) > 0 {
		firstDelim, _, _ = tagsDelims(tfDefault)
	}
	bucket = Config.Prefix + sanitizeBucket(cleanBucket) + firstDelim + normalizeTags(addTags(tags, Config.ExtraTagsHash), tfDefault)

	return &Packet{
		Bucket:      bucket,
		Value:       value,
		SrcBucket:   string(name),
		CleanBucket: cleanBucket,
		Tags:        tags,
		Modifier:    typeCode,
		Sampling:    sampling,
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
		case (c >= byte('a') && c <= byte('z')) || (c >= byte('A') && c <= byte('Z')) || (c >= byte('0') && c <= byte('9')) || c == byte('-') || c == byte('.') || c == byte('_'):
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
