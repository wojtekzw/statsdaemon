package main

// Functions connected with parsing data form original statsd format

import (
	"bytes"
	"io"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/patrickmn/go-cache"
	"time"

)

var (
	nameCache   *cache.Cache
	packetCache *cache.Cache
	usePacketCache bool = true
)

func init() {
	nameCache = cache.New(170*time.Minute, 4*time.Minute)
	packetCache = cache.New(55*time.Minute, 6*time.Minute)
}

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
	CleanBucket string
	Modifier    string
	Sampling    float32
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
		Stat.BytesReceivedInc(int64(n))
		buf = buf[:idx+n]
		if err != nil {
			if err != io.EOF {
				logCtx.Errorf("%s", err)
				Stat.ReadFailInc()
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

	var cachedPacket Packet
	if usePacketCache {
		// check if we have cached full line for counter 1  and get if from cache
		if val, found := packetCache.Get(string(line)); found {
			cachedPacket = val.(Packet)

			Stat.PointTypeInc(&cachedPacket)
			Stat.PacketCacheHit()
			return &cachedPacket
		}
	}

	Stat.PacketCacheMiss()

	logCtx := log.WithFields(log.Fields{
		"in": "parseLine",
	})

	split := bytes.SplitN(line, []byte{'|'}, 3)
	if len(split) < 2 {
		logCtx.Errorf("Failed to parse line: %s", string(line))
		Stat.PointsParseFailInc()
		return nil
	}

	keyval := split[0]
	typeCode := string(split[1]) // expected c, g, s, ms, kv

	sampling := float32(1)
	// read sampling from line
	if strings.HasPrefix(typeCode, "c") || strings.HasPrefix(typeCode, "ms") {
		if len(split) == 3 && len(split[2]) > 0 && split[2][0] == '@' {
			f64, err := strconv.ParseFloat(string(split[2][1:]), 32)
			if err != nil {
				logCtx.Errorf("Failed to ParseFloat %s (%s) in line '%s'", string(split[2][1:]), err, line)
				Stat.PointsParseFailInc()
				return nil
			}
			sampling = float32(f64)
		}
	}

	split = bytes.SplitN(keyval, []byte{':'}, 2)
	if len(split) < 2 {
		logCtx.Errorf("Failed to parse line: '%s'", line)
		Stat.PointsParseFailInc()
		return nil
	}
	// raw bucket name from line
	name := string(split[0])
	val := split[1]
	if len(val) == 0 {
		logCtx.Errorf("Failed to parse line: '%s'", line)
		Stat.PointsParseFailInc()
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
			logCtx.Errorf("Failed to ParseInt %s - %s, raw bucket: %s, line: `%s`", string(val), err, name, line)
			Stat.PointsParseFailInc()
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
			logCtx.Errorf("Failed to ParseFloat %s - %s, raw bucket: %s, line: '%s'", string(val), err, name, line)
			Stat.PointsParseFailInc()
			return nil
		}

		value = GaugeData{rel, neg, value.(float64)}
	case "s":
		value = string(val)
	case "ms":
		value, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			logCtx.Errorf("Failed to ParseFloat %s - %s, raw bucket: %s, line: '%s'", string(val), err, name, line)
			Stat.PointsParseFailInc()
			return nil
		}
	case "kv":
		value = string(val) // Key/value should not need transformation
	default:
		logCtx.Errorf("Unrecognized type code %q, raw bucket: %s, line: '%s'", typeCode, name, line)
		Stat.PointsParseFailInc()
		return nil
	}

	// parse tags from bucket name

	type bucketNames struct {
		bucket      string
		srcBucket   string
		cleanBucket string
	}

	var cachedBucket bucketNames

	if val, found := nameCache.Get(string(name)); found {
		Stat.NameCacheHit()
		cachedBucket = val.(bucketNames)
		bucket = cachedBucket.bucket
		cleanBucket = cachedBucket.cleanBucket
	} else {

		Stat.NameCacheMiss()

		tagsFromBucketName := make(map[string]string)
		cleanBucket, tagsFromBucketName, err = parseBucketAndTags(string(name))
		if err != nil {
			logCtx.Errorf("Problem parsing %s (clean version %s): %v\n", string(name), cleanBucket, err)
			Stat.PointsParseFailInc()
			return nil
		}

		// bucket is set to a name WITH tags
		firstDelim := ""
		if len(tagsFromBucketName) > 0 || len(Config.ExtraTagsHash) > 0 {
			firstDelim, _, _ = tagsDelims(tfDefault)
		}
		bucket = Config.Prefix + sanitizeBucket(cleanBucket) + firstDelim + normalizeTags(addTags(tagsFromBucketName, Config.ExtraTagsHash), tfDefault)

		cachedBucket = bucketNames{bucket: bucket, srcBucket: string(name), cleanBucket: cleanBucket}
		nameCache.Set(string(name), cachedBucket, cache.DefaultExpiration)
	}

	returnPacket := &Packet{
		Bucket:      bucket,
		Value:       value,
		CleanBucket: cleanBucket,
		Modifier:    typeCode,
		Sampling:    sampling,
	}

	Stat.PointTypeInc(returnPacket)

	if usePacketCache {
		// add full packet to cache
		if returnPacket.Modifier == "c" && returnPacket.Value.(int64) == 1 {
			packetCache.Set(string(line), *returnPacket, cache.DefaultExpiration)
		}
	}

	return returnPacket
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
