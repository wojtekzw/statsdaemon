package main

import (
	"bytes"
	"math"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"fmt"
	log "github.com/Sirupsen/logrus"
	logrus_syslog "github.com/Sirupsen/logrus/hooks/syslog"
	"github.com/bmizerany/assert"
	"github.com/boltdb/bolt"
	flag "github.com/ogier/pflag"
	"log/syslog"
)

var commonPercentiles = Percentiles{
	Percentile{
		99,
		"99",
	},
}

func init() {
	readConfig(false)
	Config.InternalLogLevel = log.FatalLevel
	// Config.InternalLogLevel = log.ErrorLevel
	log.SetLevel(Config.InternalLogLevel)
}

func removeFile(filename string) {
	if _, err := os.Stat(filename); err == nil {
		err := os.Remove(filename)
		if err != nil {
			log.Printf("Error removing file: %s", err)
		}
	}
}

func closeAndRemove(db *bolt.DB, filename string) {
	db.Close()
	removeFile(filename)
}

func TestParseBucketAndTags(t *testing.T) {
	type testCase struct {
		l     string
		valid bool
	}
	lines := []testCase{
		{l: "test.sth.^ala=kot", valid: true},
		{l: "test.sth^.ala=bad", valid: true},  //sanitized
		{l: "test.sth.^ala==bad", valid: true}, // check what is OK
	}

	for _, r := range lines {
		_, _, err := parseBucketAndTags(r.l)
		assert.Equal(t, err == nil, r.valid)
		// fmt.Printf("Bucket:%s\n", bucket)
	}

}

func TestNormalizeDot(t *testing.T) {
	type testCase struct {
		input  string
		se     bool
		output string
	}
	checks := []testCase{
		{input: "", se: false, output: ""},
		{input: "test", se: false, output: "test"},
		{input: "test.", se: false, output: "test"},
		{input: "", se: true, output: ""},
		{input: "test", se: true, output: "test."},
		{input: "test.", se: true, output: "test."},
	}

	for _, r := range checks {
		out := normalizeDot(r.input, r.se)
		assert.Equal(t, out, r.output)
	}

}

func TestMakeBucketName(t *testing.T) {
	type testCase struct {
		gp  string
		mnp string
		mn  string
		ets string
		out string
	}
	checks := []testCase{
		{gp: "", mnp: "", mn: "test", ets: "", out: "test"},
		{gp: "", mnp: "", mn: "test", ets: "end=koniec", out: "test.^end=koniec"},
		{gp: "", mnp: "prefixname", mn: "test", ets: "", out: "prefixname.test"},
		{gp: "", mnp: "prefixname.", mn: "test", ets: "", out: "prefixname.test"},
		{gp: "global", mnp: "prefixname", mn: "test", ets: "", out: "global.prefixname.test"},
		{gp: "global.", mnp: "prefixname.", mn: "test", ets: "", out: "global.prefixname.test"},
		{gp: "global", mnp: "prefixname", mn: "test", ets: "end=koniec", out: "global.prefixname.test.^end=koniec"},
		{gp: "global.", mnp: "prefixname.", mn: "test.", ets: "", out: "global.prefixname.test"},
	}

	for _, r := range checks {
		out := makeBucketName(r.gp, r.mnp, r.mn, r.ets)
		assert.Equal(t, out, r.out)
	}

}

func TestRemoveEmptyLines(t *testing.T) {
	lines := []string{"sth1", "", "sth2", ""}
	linesOut := removeEmptyLines(lines)
	assert.Equal(t, len(linesOut), 2)
	assert.Equal(t, linesOut, []string{"sth1", "sth2"})
}

func TestParseLineGauge(t *testing.T) {
	d := []byte("gaugor:333|g")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, GaugeData{false, false, 333}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gaugor:-10|g")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, GaugeData{true, true, 10}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gaugor:+4|g")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, GaugeData{true, false, 4}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	// >max(int64) && <max(uint64)
	d = []byte("gaugor:18446744073709551606|g")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, GaugeData{false, false, 18446744073709551606}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestParseLineCount(t *testing.T) {
	d := []byte("gorets:2|c|@0.1")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(2), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)

	d = []byte("gorets:4|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:-4|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(-4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	// check packet cache
	d = []byte("gorets:1|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:1|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)


	// check packet cache
	d = []byte("gorets:-1|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(-1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:-1|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(-1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)


}

func TestPacketCacheInParseLineCount(t *testing.T) {
	d := []byte("gorets:1|c|@0.1")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)


	d = []byte("gorets:1|c|@0.1")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)


	d = []byte("gorets:1|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:1|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:-1|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(-1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:-1|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(-1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)



	d = []byte("a.key.with-0.dash.^9key=val9.^1key=val1.^7key=val7:1|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "a.key.with-0.dash.^1key=val1.^7key=val7.^9key=val9", packet.Bucket)
	assert.Equal(t, int64(1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)


	d = []byte("a.key.with-0.dash.^9key=val9.^1key=val1.^7key=val7:1|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "a.key.with-0.dash.^1key=val1.^7key=val7.^9key=val9", packet.Bucket)
	assert.Equal(t, int64(1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash.^9key=val9.^1key=val1.^7key=val7:-1|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "a.key.with-0.dash.^1key=val1.^7key=val7.^9key=val9", packet.Bucket)
	assert.Equal(t, int64(-1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash.^9key=val9.^1key=val1.^7key=val7:-1|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "a.key.with-0.dash.^1key=val1.^7key=val7.^9key=val9", packet.Bucket)
	assert.Equal(t, int64(-1), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)


}


func TestParseLineTimer(t *testing.T) {
	d := []byte("glork:320|ms")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, float64(320), packet.Value.(float64))
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("glork:320|ms|@0.1")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, float64(320), packet.Value.(float64))
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)
}

func TestParseLineSet(t *testing.T) {
	d := []byte("uniques:765|s")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "uniques", packet.Bucket)
	assert.Equal(t, "765", packet.Value)
	assert.Equal(t, "s", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestParseLineMisc(t *testing.T) {
	d := []byte("a.key.with-0.dash:4|c")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with 0.space:4|c")
	packet = parseLine(d)
	assert.Equal(t, "a.key.with_0.space", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with/0.slash:4|c")
	packet = parseLine(d)
	assert.Equal(t, "a.key.with-0.slash", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with@#*&%$^_0.garbage:4|c")
	packet = parseLine(d)
	assert.Equal(t, "a.key.with_0.garbage", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	flag.Set("prefix", "test.")

	d = []byte("prefix:4|c")
	packet = parseLine(d)
	assert.Equal(t, "test.prefix", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
	flag.Set("prefix", "")

	d = []byte("counter0:11|c|@0.001\na.key.with-0.dash:4|c\ngauge:3|g\ncounter:10|c|@0.01")
	parser := NewParser(bytes.NewBuffer(d), true)

	packet, more := parser.Next()
	assert.Equal(t, more, true)
	assert.Equal(t, "counter0", packet.Bucket)
	assert.Equal(t, int64(11), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(0.001), packet.Sampling)

	packet, more = parser.Next()
	assert.Equal(t, more, true)
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	packet, more = parser.Next()
	assert.Equal(t, more, true)
	assert.Equal(t, "gauge", packet.Bucket)
	assert.Equal(t, GaugeData{false, false, 3}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	packet, more = parser.Next()
	assert.Equal(t, more, false)
	assert.Equal(t, "counter", packet.Bucket)
	assert.Equal(t, int64(10), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(0.01), packet.Sampling)

	d = []byte("a.key.with-0.dash:4\ngauge3|g")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("a.key.with-0.dash:4")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:5m")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:5|mg")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:5|ms|@")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:xxx|c")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gaugor:xxx|g")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gaugor:xxx|z")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("deploys.test.myservice4:100|t")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("up-to-colon:")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("up-to-pipe:1|")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}
}

func TestParseLineTags(t *testing.T) {
	d := []byte("a.key.with-0.dash.^key=val1.^key=val2:4|c")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "a.key.with-0.dash.^key=val2", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash.^9key=val9.^1key=val1.^7key=val7:4|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "a.key.with-0.dash.^1key=val1.^7key=val7.^9key=val9", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)


	d = []byte("a.key.with-0.dash.^key2=val2...^key1=val1:4|c")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("a.key.with-0.dash.^key2=val2.^key1=val1..:4|c")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("a.key.with-0.dash.:4|c")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("a.key.with-0.dash..:4|c")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("a.key.with-0.dash.^key=:val:4|c")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("a.key.with-0.dash.^key=:val.^key2=val2:4|c")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("a.key.with-0.dash.^key=:10.^key2=val2:4|c")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

}

func TestMultiLine(t *testing.T) {
	b := bytes.NewBuffer([]byte("a.key.with-0.dash:4|c\ngauge:3|g"))
	parser := NewParser(b, true)

	packet, more := parser.Next()
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, more, true)
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	packet, more = parser.Next()
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, more, false)
	assert.Equal(t, "gauge", packet.Bucket)
	assert.Equal(t, GaugeData{false, false, 3}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestPacketHandlerReceiveCounter(t *testing.T) {

	Stat.curStat.PointsReceived = 0
	Config.ResetCounters = true

	p := &Packet{
		Bucket:   "gorets",
		Value:    int64(100),
		Modifier: "c",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, Stat.curStat.PointsReceived, int64(1))

	packetHandler(p)
	assert.Equal(t, Stat.curStat.PointsReceived, int64(2))
}

func TestPacketHandlerCount(t *testing.T) {
	counters = make(map[string]int64)

	p := &Packet{
		Bucket:   "gorets",
		Value:    int64(100),
		Modifier: "c",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, counters["gorets"], int64(100))

	p.Value = int64(3)
	packetHandler(p)
	assert.Equal(t, counters["gorets"], int64(103))

	p.Value = int64(-4)
	packetHandler(p)
	assert.Equal(t, counters["gorets"], int64(99))

	p.Value = int64(-100)
	packetHandler(p)
	assert.Equal(t, counters["gorets"], int64(-1))
}

func TestPacketHandlerGauge(t *testing.T) {
	gauges = make(map[string]float64)

	p := &Packet{
		Bucket:   "gaugor",
		Value:    GaugeData{false, false, 333},
		Modifier: "g",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], float64(333))

	// -10
	p.Value = GaugeData{true, true, 10}
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], float64(323))

	// +4
	p.Value = GaugeData{true, false, 4}
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], float64(327))

	// <0 overflow
	p.Value = GaugeData{false, false, 10}
	packetHandler(p)
	p.Value = GaugeData{true, true, 20}
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], float64(0))

	// >2^64 overflow
	p.Value = GaugeData{false, false, float64(math.MaxFloat64 - 10)}
	packetHandler(p)
	p.Value = GaugeData{true, false, 20}
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], float64(math.MaxFloat64))
}

func TestPacketHandlerTimer(t *testing.T) {
	timers = make(map[string]Float64Slice)

	p := &Packet{
		Bucket:   "glork",
		Value:    float64(320),
		Modifier: "ms",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, len(timers["glork"]), 1)
	assert.Equal(t, timers["glork"][0], float64(320))

	p.Value = float64(100)
	packetHandler(p)
	assert.Equal(t, len(timers["glork"]), 2)
	assert.Equal(t, timers["glork"][1], float64(100))
}

func TestPacketHandlerSet(t *testing.T) {
	sets = make(map[string][]string)

	p := &Packet{
		Bucket:   "uniques",
		Value:    "765",
		Modifier: "s",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, len(sets["uniques"]), 1)
	assert.Equal(t, sets["uniques"][0], "765")

	p.Value = "567"
	packetHandler(p)
	assert.Equal(t, len(sets["uniques"]), 2)
	assert.Equal(t, sets["uniques"][1], "567")
}

func TestProcessCounters(t *testing.T) {

	Config.PersistCountKeys = int64(10)
	counters = make(map[string]int64)
	var buffer bytes.Buffer
	now := int64(1418052649)

	counters["gorets"] = int64(123)

	var err error
	Config.StoreDb = "/tmp/stats_test.db"
	removeFile(Config.StoreDb)

	dbHandle, err = bolt.Open(Config.StoreDb, 0644, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatalf("Error opening %s (%s)\n", Config.StoreDb, err)
	}

	defer closeAndRemove(dbHandle, Config.StoreDb)

	num := processCounters(&buffer, now, true, "external", dbHandle)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "gorets 123 1418052649\n")

	// run processCounters() enough times to make sure it purges items
	for i := 0; i < int(Config.PersistCountKeys)+10; i++ {
		num = processCounters(&buffer, now, true, "external", dbHandle)
	}
	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	// expect two more lines - the good one and an empty one at the end
	assert.Equal(t, len(lines), int(Config.PersistCountKeys+2))
	assert.Equal(t, string(lines[0]), "gorets 123 1418052649")
	assert.Equal(t, string(lines[Config.PersistCountKeys]), "gorets 0 1418052649")
}

func TestProcessTimers(t *testing.T) {
	// Some data with expected mean of 20
	timers = make(map[string]Float64Slice)
	timers["response_time"] = []float64{0, 30, 30}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := processTimers(&buffer, now, Percentiles{}, "external")

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "response_time.mean 20.000000 1418052649")
	assert.Equal(t, string(lines[1]), "response_time.upper 30.000000 1418052649")
	assert.Equal(t, string(lines[2]), "response_time.lower 0.000000 1418052649")
	assert.Equal(t, string(lines[3]), "response_time.count 3 1418052649")

	num = processTimers(&buffer, now, Percentiles{}, "external")
	assert.Equal(t, num, int64(0))
}

func TestProcessGauges(t *testing.T) {
	// Some data with expected mean of 20
	flag.Set("delete-gauges", "false")
	gauges = make(map[string]float64)
	gauges["gaugor"] = math.MaxUint64

	now := int64(1418052649)

	var buffer bytes.Buffer

	num := processGauges(&buffer, now, "external")
	assert.Equal(t, num, int64(0))
	assert.Equal(t, buffer.String(), "")

	gauges["gaugor"] = 12345
	num = processGauges(&buffer, now, "external")
	assert.Equal(t, num, int64(1))

	gauges["gaugor"] = math.MaxUint64
	num = processGauges(&buffer, now, "external")
	assert.Equal(t, buffer.String(), "gaugor 12345.000000 1418052649\ngaugor 12345.000000 1418052649\n")
	assert.Equal(t, num, int64(1))
}

func TestProcessDeleteGauges(t *testing.T) {
	// Some data with expected mean of 20
	flag.Set("delete-gauges", "true")
	gauges = make(map[string]float64)
	gauges["gaugordelete"] = math.MaxUint64

	now := int64(1418052649)

	var buffer bytes.Buffer

	num := processGauges(&buffer, now, "external")
	assert.Equal(t, num, int64(0))
	assert.Equal(t, buffer.String(), "")

	gauges["gaugordelete"] = 12345
	num = processGauges(&buffer, now, "external")
	assert.Equal(t, num, int64(1))

	gauges["gaugordelete"] = math.MaxUint64
	num = processGauges(&buffer, now, "external")
	assert.Equal(t, buffer.String(), "gaugordelete 12345.000000 1418052649\n")
	assert.Equal(t, num, int64(0))
}

func TestProcessSets(t *testing.T) {
	sets = make(map[string][]string)

	now := int64(1418052649)

	var buffer bytes.Buffer

	// three unique values
	sets["uniques"] = []string{"123", "234", "345"}
	num := processSets(&buffer, now, "external")
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "uniques 3 1418052649\n")

	// one value is repeated
	buffer.Reset()
	sets["uniques"] = []string{"123", "234", "234"}
	num = processSets(&buffer, now, "external")
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "uniques 2 1418052649\n")

	// make sure sets are purged
	num = processSets(&buffer, now, "external")
	assert.Equal(t, num, int64(0))
}

func TestProcessTimersUpperPercentile(t *testing.T) {
	// Some data with expected 75% of 2
	timers = make(map[string]Float64Slice)
	timers["response_time"] = []float64{0, 1, 2, 3}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := processTimers(&buffer, now, Percentiles{
		Percentile{
			75,
			"75",
		},
	}, "external")

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "response_time.upper_75 2.000000 1418052649")
}

func TestProcessTimesLowerPercentile(t *testing.T) {
	timers = make(map[string]Float64Slice)
	timers["time"] = []float64{0, 1, 2, 3}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := processTimers(&buffer, now, Percentiles{
		Percentile{
			-75,
			"-75",
		},
	}, "external")

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "time.lower_75 1.000000 1418052649")
}

func TestMultipleUDPSends(t *testing.T) {
	addr := "127.0.0.1:8126"

	address, _ := net.ResolveUDPAddr("udp", addr)
	listener, err := net.ListenUDP("udp", address)
	assert.Equal(t, nil, err)

	ch := make(chan *Packet, maxUnprocessedPackets)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		parseTo(listener, false, ch)
		wg.Done()
	}()

	conn, err := net.DialTimeout("udp", addr, 50*time.Millisecond)
	assert.Equal(t, nil, err)

	n, err := conn.Write([]byte("deploys.test.myservice:2|c|@0.0001"))
	assert.Equal(t, nil, err)
	assert.Equal(t, len("deploys.test.myservice:2|c|@0.0001"), n)

	// error in format - not found below in select
	n, err = conn.Write([]byte("deploys.test.my:service:2|c"))

	n, err = conn.Write([]byte("deploys.test.myservice.new:1|c"))
	assert.Equal(t, nil, err)
	assert.Equal(t, len("deploys.test.myservice.new:1|c"), n)

	select {
	case pack := <-ch:
		assert.Equal(t, "deploys.test.myservice", pack.Bucket)
		assert.Equal(t, int64(2), pack.Value.(int64))
		assert.Equal(t, "c", pack.Modifier)
		assert.Equal(t, float32(0.0001), pack.Sampling)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("packet receive timeout")
	}

	select {
	case pack := <-ch:
		assert.Equal(t, "deploys.test.myservice.new", pack.Bucket)
		assert.Equal(t, int64(1), pack.Value.(int64))
		assert.Equal(t, "c", pack.Modifier)
		assert.Equal(t, float32(1), pack.Sampling)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("packet receive timeout")
	}

	listener.Close()
	wg.Wait()
}

//func BenchmarkManyDifferentSensors(t *testing.B) {
//	r := rand.New(rand.NewSource(438))
//	for i := 0; i < 1000; i++ {
//		bucket := "response_time" + strconv.Itoa(i)
//		for i := 0; i < 10000; i++ {
//			a := float64(r.Uint32() % 1000)
//			timers[bucket] = append(timers[bucket], a)
//		}
//	}
//
//	for i := 0; i < 1000; i++ {
//		bucket := "count" + strconv.Itoa(i)
//		for i := 0; i < 10000; i++ {
//			a := int64(r.Uint32() % 1000)
//			counters[bucket] = a
//		}
//	}
//
//	for i := 0; i < 1000; i++ {
//		bucket := "gauge" + strconv.Itoa(i)
//		for i := 0; i < 10000; i++ {
//			a := float64(r.Uint32() % 1000)
//			gauges[bucket] = a
//		}
//	}
//
//	var (
//		buff bytes.Buffer
//		err  error
//	)
//
//	Config.StoreDb = "/tmp/stats_test.db"
//	removeFile(Config.StoreDb)
//
//	dbHandle, err = bolt.Open(Config.StoreDb, 0644, &bolt.Options{Timeout: 1 * time.Second})
//	if err != nil {
//		log.Fatalf("Error opening %s (%s)\n", Config.StoreDb, err)
//	}
//	// dbHandle.NoSync = true
//
//	defer closeAndRemove(dbHandle, Config.StoreDb)
//
//	now := time.Now().Unix()
//	t.ResetTimer()
//	processTimers(&buff, now, commonPercentiles, "external")
//	processCounters(&buff, now, true, "external", dbHandle)
//	processGauges(&buff, now, "external")
//}
//
//func BenchmarkOneBigTimer(t *testing.B) {
//	r := rand.New(rand.NewSource(438))
//	bucket := "response_time"
//	for i := 0; i < 10000000; i++ {
//		a := float64(r.Uint32() % 1000)
//		timers[bucket] = append(timers[bucket], a)
//	}
//
//	var buff bytes.Buffer
//	t.ResetTimer()
//	processTimers(&buff, time.Now().Unix(), commonPercentiles, "external")
//}
//
//func BenchmarkLotsOfTimers(t *testing.B) {
//	r := rand.New(rand.NewSource(438))
//	for i := 0; i < 1000; i++ {
//		bucket := "response_time" + strconv.Itoa(i)
//		for i := 0; i < 10000; i++ {
//			a := float64(r.Uint32() % 1000)
//			timers[bucket] = append(timers[bucket], a)
//		}
//	}
//
//	var buff bytes.Buffer
//	t.ResetTimer()
//	processTimers(&buff, time.Now().Unix(), commonPercentiles, "external")
//}
//
//func BenchmarkOneTimerWith10Points(t *testing.B) {
//	r := rand.New(rand.NewSource(438))
//	bucket := "response_time"
//	for i := 0; i < 10; i++ {
//		a := float64(r.Uint32() % 1000)
//		timers[bucket] = append(timers[bucket], a)
//	}
//
//	var buff bytes.Buffer
//	t.ResetTimer()
//
//	for i := 0; i < t.N; i++ {
//		processTimers(&buff, time.Now().Unix(), commonPercentiles, "external")
//		buff = bytes.Buffer{}
//	}
//
//}
//
//func Benchmark100CountersWith1000IncrementsEach(t *testing.B) {
//	r := rand.New(rand.NewSource(438))
//
//	for i := 0; i < 100; i++ {
//		bucket := "count" + strconv.Itoa(i)
//		for i := 0; i < 1000; i++ {
//			a := int64(r.Uint32() % 1000)
//			counters[bucket] = a
//		}
//	}
//
//	var (
//		buff bytes.Buffer
//		err  error
//	)
//
//	Config.StoreDb = "/tmp/stats_test.db"
//	removeFile(Config.StoreDb)
//
//	dbHandle, err = bolt.Open(Config.StoreDb, 0644, &bolt.Options{Timeout: 1 * time.Second})
//	if err != nil {
//		log.Fatalf("Error opening %s (%s)\n", Config.StoreDb, err)
//	}
//	// dbHandle.NoSync = true
//
//	defer closeAndRemove(dbHandle, Config.StoreDb)
//
//	t.ResetTimer()
//
//	for i := 0; i < t.N; i++ {
//		processCounters(&buff, time.Now().Unix(), true, "external", dbHandle)
//		buff = bytes.Buffer{}
//	}
//
//}


func BenchmarkParseLineRateCache(b *testing.B) {
	usePacketCache = true
	d := []byte("a.key.with-0.dash:1|c|@0.5")
	for i := 0; i < b.N; i++ {
		parseLine(d)
	}
}

func BenchmarkParseLineWith5TagsCache(b *testing.B) {
	usePacketCache = true
	d := []byte("a.key.with-0.dash.^host=dev.^env=prod.^product=sth.^key1=val1.^key2=val2:1|c|@0.5")
	for i := 0; i < b.N; i++ {
		parseLine(d)
	}
}

func BenchmarkParseLineWith1TagCorrectCache(b *testing.B) {
	usePacketCache = true
	d := []byte("cache.hit.^con=en:1|c")
	for i := 0; i < b.N; i++ {
		parseLine(d)
	}
}

func BenchmarkParseLineRateNoCache(b *testing.B) {
	usePacketCache = false
	d := []byte("a.key.with-0.dash:1|c|@0.5")
	for i := 0; i < b.N; i++ {
		parseLine(d)
	}
}

func BenchmarkParseLineWith5TagsNoCache(b *testing.B) {
	usePacketCache = false
	d := []byte("a.key.with-0.dash.^host=dev.^env=prod.^product=sth.^key1=val1.^key2=val2:1|c|@0.5")
	for i := 0; i < b.N; i++ {
		parseLine(d)
	}
}



func BenchmarkParseLineWith1TagCorrectNoCache(b *testing.B) {
	usePacketCache = false
	d := []byte("cache.hit.^con=en:1|c")
	for i := 0; i < b.N; i++ {
		parseLine(d)
	}
}
func BenchmarkParseLineWith1TagError(b *testing.B) {
	d := []byte("cache.hit.^con=en::1|c")
	for i := 0; i < b.N; i++ {
		parseLine(d)
	}
}

func BenchmarkParseLineWith1TagErrorAndSyslog(b *testing.B) {
	hook, err := logrus_syslog.NewSyslogHook("", "", syslog.LOG_DEBUG|syslog.LOG_LOCAL3, "statsdaemon_test")
	if err != nil {
		fmt.Printf("Unable to connect to syslog daemon: %s\n", err)
	} else {
		log.AddHook(hook)
		log.SetFormatter(&log.JSONFormatter{})
	}

	b.ResetTimer()
	d := []byte("cache.hit.^con=en::1|c")
	for i := 0; i < b.N; i++ {
		parseLine(d)
	}
}

func BenchmarkPacketHandlerCounter(b *testing.B) {
	counters = make(map[string]int64)

	p := &Packet{
		Bucket:   "gorets",
		Value:    int64(1),
		Modifier: "c",
		Sampling: float32(1),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packetHandler(p)
	}

}


func BenchmarkPacketHandlerGauge(b *testing.B) {
	gauges = make(map[string]float64)

	p := &Packet{
		Bucket:   "gaugor",
		Value:    GaugeData{false, false, 333},
		Modifier: "g",
		Sampling: float32(1),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packetHandler(p)
	}
}


func BenchmarkPacketHandlerTimer(b *testing.B) {
	timers = make(map[string]Float64Slice)

	p := &Packet{
		Bucket:   "glork",
		Value:    float64(320),
		Modifier: "ms",
		Sampling: float32(1),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packetHandler(p)
	}
}


func BenchmarkPacketHandlerSet(b *testing.B) {
	sets = make(map[string][]string)

	p := &Packet{
		Bucket:   "uniques",
		Value:    "765",
		Modifier: "s",
		Sampling: float32(1),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packetHandler(p)
	}

}


func BenchmarkReadStructChan(b *testing.B) {
	ch := make(chan struct{}, 1000000)
	b.ResetTimer()
	go func() {
		for {
			ch <- struct{}{}
		}
	}()

	for i := 0; i < b.N; i++ {
		<-ch
	}
}
