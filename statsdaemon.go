package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/boltdb/bolt"
)

// Network constants
const (
	MAX_UNPROCESSED_PACKETS = 1000
	TCP_READ_SIZE           = 4096
)

var signalchan chan os.Signal

// Global vars for command line flags
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
	storeDb           = flag.String("store-db", "/tmp/statsdaemon.db", "Name of databse for permanent counters storage (for conversion rate to counter)")
	percentThreshold  = Percentiles{}
	prefix            = flag.String("prefix", "", "Prefix for all stats")
	postfix           = flag.String("postfix", "", "Postfix for all stats (can be used to add default tags)")
)

func init() {
	flag.Var(&percentThreshold, "percent-threshold",
		"percentile calculation for timers (0-100, may be given multiple times)")
}

// Global var used for all metrics
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
	dbHandle        *bolt.DB
)

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

	dbHandle, err = bolt.Open(*storeDb, 0644, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatalf("Error opening %s (%s)\n", *storeDb, err)
	}
	// dbHandle.NoSync = true

	defer dbHandle.Close()

	go udpListener()
	if *tcpServiceAddress != "" {
		go tcpListener()
	}
	monitor()
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
