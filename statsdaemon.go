package main

import (
	"fmt"
	"io"

	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
	"github.com/jinzhu/configor"
	flag "github.com/ogier/pflag"
	"gopkg.in/yaml.v2"
)

// Network constants & dbName
const (
	maxUnprocessedPackets = 10000

	tcpReadSize        = 4096
	maxUDPPacket       = 1472
	flushInterval      = 10
	dbPath             = "/tmp/statsdaemon.db"
	configPath         = "/etc/statsdaemon/statsdaemon.yml"
	receiveCounterName = "statsdaemon.metrics.count"

	defaultGraphiteAddress = "127.0.0.1:2003"
	defaultOpenTSDBAddress = "127.0.0.1:4242"

	defaultUDPServiceAddress = ":8125"
	defaultTCPServiceAddress = ""

	defaultBackendType = "external"
)

type ConfigApp struct {
	UDPServiceAddress string      `yaml:"udp-addr"`
	TCPServiceAddress string      `yaml:"tcp-addr"`
	MaxUDPPacketSize  int64       `yaml:"max-udp-packet-size"`
	BackendType       string      `yaml:"backend-type"`
	PostFlushCmd      string      `yaml:"post-flush-cmd"`
	GraphiteAddress   string      `yaml:"graphite"`
	OpenTSDBAddress   string      `yaml:"opentsdb"`
	FlushInterval     int64       `yaml:"flush-interval"`
	Debug             bool        `yaml:"debug"`
	ShowVersion       bool        `yaml:"-"`
	DeleteGauges      bool        `yaml:"delete-gauges"`
	ResetCounters     bool        `yaml:"reset-counters"`
	PersistCountKeys  int64       `yaml:"persist-count-keys"`
	ReceiveCounter    string      `yaml:"receive-counter"`
	StoreDb           string      `yaml:"store-db"`
	Prefix            string      `yaml:"prefix"`
	ExtraTags         string      `yaml:"extra-tags"`
	PercentThreshold  Percentiles `yaml:"-"` // `yaml:"percent-threshold,omitempty"`
	PrintConfig       bool        `yaml:"-"`
	LogName           string      `yaml:"log-name"`
	// private - calculated below
	ExtraTagsHash          map[string]string `yaml:"-"`
	ReceiveCounterWithTags string            `yaml:"-"`
}

// Global vars for command line flags
var (
	configFile *string
	Config     = ConfigApp{}
	ConfigYAML = ConfigApp{}

	signalchan chan os.Signal // for signal exits
)

func readConfig(parse bool) {
	var err error
	// Set defaults
	ConfigYAML.UDPServiceAddress = defaultUDPServiceAddress
	ConfigYAML.TCPServiceAddress = defaultTCPServiceAddress
	ConfigYAML.MaxUDPPacketSize = maxUDPPacket
	ConfigYAML.BackendType = defaultBackendType
	ConfigYAML.PostFlushCmd = "stdout"
	ConfigYAML.GraphiteAddress = defaultGraphiteAddress
	ConfigYAML.OpenTSDBAddress = defaultOpenTSDBAddress
	ConfigYAML.FlushInterval = flushInterval
	ConfigYAML.Debug = false
	ConfigYAML.ShowVersion = false
	ConfigYAML.DeleteGauges = true
	ConfigYAML.ResetCounters = true
	ConfigYAML.PersistCountKeys = 0
	ConfigYAML.ReceiveCounter = receiveCounterName
	ConfigYAML.StoreDb = dbPath
	ConfigYAML.Prefix = ""
	ConfigYAML.ExtraTags = ""
	ConfigYAML.PercentThreshold = Percentiles{}
	ConfigYAML.PrintConfig = false
	ConfigYAML.LogName = "stdout"

	configFile = flag.String("config", configPath, "Configuration file name (warning not error if not exists)")
	flag.StringVar(&Config.UDPServiceAddress, "udp-addr", ConfigYAML.UDPServiceAddress, "UDP listen service address")
	flag.StringVar(&Config.TCPServiceAddress, "tcp-addr", ConfigYAML.TCPServiceAddress, "TCP listen service address, if set")
	flag.Int64Var(&Config.MaxUDPPacketSize, "max-udp-packet-size", ConfigYAML.MaxUDPPacketSize, "Maximum UDP packet size")
	flag.StringVar(&Config.BackendType, "backend-type", ConfigYAML.BackendType, "MANDATORY: Backend to use: graphite, opentsdb, external")
	flag.StringVar(&Config.PostFlushCmd, "post-flush-cmd", ConfigYAML.PostFlushCmd, "Command to run on each flush")
	flag.StringVar(&Config.GraphiteAddress, "graphite", ConfigYAML.GraphiteAddress, "Graphite service address")
	flag.StringVar(&Config.OpenTSDBAddress, "opentsdb", ConfigYAML.OpenTSDBAddress, "OpenTSDB service address")
	flag.Int64Var(&Config.FlushInterval, "flush-interval", ConfigYAML.FlushInterval, "Flush interval (seconds)")
	flag.BoolVar(&Config.Debug, "debug", ConfigYAML.Debug, "Print statistics sent to backend")
	flag.BoolVar(&Config.ShowVersion, "version", ConfigYAML.ShowVersion, "Print version string")
	flag.BoolVar(&Config.DeleteGauges, "delete-gauges", ConfigYAML.DeleteGauges, "Don't send values to graphite for inactive gauges, as opposed to sending the previous value")
	flag.BoolVar(&Config.ResetCounters, "reset-counters", ConfigYAML.ResetCounters, "Reset counters after sending value to backend (send rate) or  send cumulated value (artificial counter - eg. for OpenTSDB & Grafana)")
	flag.Int64Var(&Config.PersistCountKeys, "persist-count-keys", ConfigYAML.PersistCountKeys, "Number of flush-intervals to persist count keys")
	flag.StringVar(&Config.ReceiveCounter, "receive-counter", ConfigYAML.ReceiveCounter, "Metric name for total metrics received per interval (no prefix,postfix added, only extra-tags)")
	flag.StringVar(&Config.StoreDb, "store-db", ConfigYAML.StoreDb, "Name of database for permanent counters storage (for conversion from rate to counter)")
	flag.StringVar(&Config.Prefix, "prefix", ConfigYAML.Prefix, "Prefix for all stats")
	flag.StringVar(&Config.ExtraTags, "extra-tags", ConfigYAML.ExtraTags, "Default tags added to all measures in format: tag1=value1 tag2=value2")
	// flag.Var(&Config.PercentThreshold, "percent-threshold", "Percentile calculation for timers (0-100, may be given multiple times)")
	flag.BoolVar(&Config.PrintConfig, "print-config", ConfigYAML.PrintConfig, "Print config in YAML format")
	flag.StringVar(&Config.LogName, "log-name", ConfigYAML.LogName, "Name of file to log into. If empty or \"stdout\" than logs to stdout")

	if parse {
		flag.Parse()
	}

	os.Setenv("CONFIGOR_ENV_PREFIX", "SD")

	if len(*configFile) > 0 {
		if _, err = os.Stat(*configFile); os.IsNotExist(err) {
			fmt.Printf("# Warning: No config file: %s\n", *configFile)
			*configFile = ""
		}

		if len(*configFile) > 0 {
			err = configor.Load(&ConfigYAML, *configFile)
			if err != nil {
				fmt.Printf("Error loading config file: %s\n", err)
			} else {
				// set configs read form YAML file
				// Overwites flags
				Config = ConfigYAML
			}
		}

		// visitor := func(a *flag.Flag) {
		// 	fmt.Println(">", a.Name, "value=", a.Value)
		// 	switch a.Name {
		// 	case "print-config", "version":
		// 		break
		// 	case "udp-addr":
		// 		ConfigYAML.UDPServiceAddress = a.Value.(string)
		// 	default:
		// 		fmt.Printf("Internal Config Error - unknown variable: %s\n", a.Name)
		// 		os.Exit(1)
		// 	}
		//
		// }
		// flag.Visit(visitor)
	}

	// calculate extraFlags hash
	Config.ExtraTagsHash, err = parseExtraTags(Config.ExtraTags)
	if err != nil {
		fmt.Printf("Extra Tags: \"%s\" - %s\n", Config.ExtraTags, err)
		os.Exit(1)
	}
	firstDelim := ""
	if len(Config.ExtraTagsHash) > 0 {
		firstDelim, _, _ = tagsDelims(tfDefault)
	}
	Config.ReceiveCounterWithTags = Config.ReceiveCounter + firstDelim + normalizeTags(Config.ExtraTagsHash, tfDefault)

	if Config.LogName == "" {
		Config.LogName = "stdout"
	}

}

// Global var used for all metrics
var (
	In              = make(chan *Packet, maxUnprocessedPackets)
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
	logFile         io.Writer
)

func main() {
	readConfig(true)
	err := validateConfig()
	if err != nil {
		fmt.Printf("\n%s\n\n", err)
		flag.Usage()
		os.Exit(1)
	}

	if Config.ShowVersion {
		fmt.Printf("statsdaemon v%s (built w/%s)\n", VERSION, runtime.Version())
		os.Exit(0)
	}

	if Config.PrintConfig {
		out, _ := yaml.Marshal(Config)
		fmt.Printf("# Default config file in YAML based on config options\n%s", string(out))
		os.Exit(0)
	}

	if Config.LogName == "stdout" {
		log.SetOutput(os.Stdout)
	} else {
		logFile, err := os.OpenFile(Config.LogName, os.O_WRONLY|os.O_CREATE, 0640)
		if err != nil {
			fmt.Printf("Error opennig log file: %s\n", err)
			os.Exit(1)
		}
		log.SetOutput(logFile)
	}

	signalchan = make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGTERM, syscall.SIGQUIT)

	dbHandle, err = bolt.Open(Config.StoreDb, 0644, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.WithFields(log.Fields{
			"in":    "main",
			"after": "Open",
			"ctx":   "Bolt DB open",
		}).Fatalf("%s", err)
	}
	// dbHandle.NoSync = true

	defer dbHandle.Close()

	go udpListener()
	if Config.TCPServiceAddress != "" {
		go tcpListener()
	}
	monitor()
}

func validateConfig() error {
	// FIXME  check all params/flags
	doNotCheckBackend := Config.PrintConfig || Config.ShowVersion
	if Config.BackendType == "" && !doNotCheckBackend {
		return fmt.Errorf("Parameter error: backend-type can't be empty")
	}
	if Config.BackendType != "external" && Config.BackendType != "graphite" && Config.BackendType != "opentsdb" && !doNotCheckBackend {
		return fmt.Errorf("Parameter error: Invalid backend-type: %s", Config.BackendType)
	}

	if (Config.GraphiteAddress == "-" || Config.GraphiteAddress == "") && Config.BackendType == "graphite" {
		return fmt.Errorf("Parameter error: Graphite backend selected and no graphite server address")
	}

	if (Config.OpenTSDBAddress == "-" || Config.OpenTSDBAddress == "") && Config.BackendType == "opentsdb" {
		return fmt.Errorf("Parameter error: OpenTSDB backend selected and no OpenTSDB server address")
	}
	return nil
}

func udpListener() {
	logCtx := log.WithFields(log.Fields{
		"in":    "udpListener",
		"after": "ResolveUDPAddr",
		"ctx":   "Start UDP listener",
	})

	address, err := net.ResolveUDPAddr("udp", Config.UDPServiceAddress)
	if err != nil {
		logCtx.Fatalf("%s", err)
	}
	logCtx.Infof("Listening on %s", address)

	listener, err := net.ListenUDP("udp", address)
	if err != nil {

		logCtx.WithField("after", "ListenUDP").Fatalf("%s", err)
	}

	parseTo(listener, false, In)
}

func tcpListener() {
	logCtx := log.WithFields(log.Fields{
		"in":    "tcpListener",
		"after": "ResolveTCPAddr",
		"ctx":   "Start TCP listener",
	})
	address, err := net.ResolveTCPAddr("tcp", Config.TCPServiceAddress)
	if err != nil {
		logCtx.Fatalf("%s", err)
	}
	logCtx.Infof("listening on %s", address)
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		logCtx.WithField("after", "ListenTCP").Fatalf("%s", err)
	}
	defer listener.Close()

	logCtx = log.WithFields(log.Fields{
		"in":    "tcpListener",
		"after": "AcceptTCP",
		"ctx":   "Accept TCP loop",
	})

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			logCtx.Fatalf("%s", err)
		}
		go parseTo(conn, true, In)
	}
}

func monitor() {
	logCtx := log.WithFields(log.Fields{
		"in":    "monitor",
		"after": "submit",
		"ctx":   "Main processing loop",
	})

	period := time.Duration(Config.FlushInterval) * time.Second
	ticker := time.NewTicker(period)
	for {
		select {
		case sig := <-signalchan:
			logCtx.WithField("after", "signal").Infof("Caught signal \"%v\"... shutting down", sig)
			if err := submit(time.Now().Add(period), Config.BackendType); err != nil {
				logCtx.Errorf("%s", err)
			}
			return
		case <-ticker.C:
			if err := submit(time.Now().Add(period), Config.BackendType); err != nil {
				logCtx.Errorf("%s", err)
			}
		case s := <-In:
			packetHandler(s)
		}
	}
}
