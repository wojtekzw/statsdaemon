package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log/syslog"

	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	logrus_syslog "github.com/Sirupsen/logrus/hooks/syslog"
	"github.com/boltdb/bolt"
	"github.com/jinzhu/configor"
	flag "github.com/ogier/pflag"
	"gopkg.in/yaml.v2"
)

// Network constants & dbName
const (
	maxUnprocessedPackets = 10000

	tcpReadSize     = 4096
	maxUDPPacket    = 1432
	flushInterval   = 10
	dbPath          = "/tmp/statsdaemon.db"
	configPath      = "/etc/statsdaemon/statsdaemon.yml"
	statsPrefixName = "statsdaemon"

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
	LogLevel          string      `yaml:"log-level"`
	DeleteGauges      bool        `yaml:"delete-gauges"`
	ResetCounters     bool        `yaml:"reset-counters"`
	PersistCountKeys  int64       `yaml:"persist-count-keys"`
	StatsPrefix       string      `yaml:"stats-prefix"`
	StoreDb           string      `yaml:"store-db"`
	Prefix            string      `yaml:"prefix"`
	ExtraTags         string      `yaml:"extra-tags"`
	PercentThreshold  Percentiles `yaml:"percent-threshold"` // `yaml:"percent-threshold,omitempty"`
	ShowVersion       bool        `yaml:"-"`
	PrintConfig       bool        `yaml:"-"`
	LogName           string      `yaml:"log-name"`
	LogToSyslog       bool        `yaml:"log-to-syslog"`
	SyslogUDPAddress  string      `yaml:"syslog-udp-address"`
	// private - calculated below
	ExtraTagsHash    map[string]string `yaml:"-"`
	InternalLogLevel log.Level         `yaml:"-"`
}

// Global vars for command line flags
var (
	configFile *string
	Config     = ConfigApp{}
	ConfigYAML = ConfigApp{}
	Stat       = DaemonStat{}

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
	ConfigYAML.LogLevel = "error"
	ConfigYAML.ShowVersion = false
	ConfigYAML.DeleteGauges = true
	ConfigYAML.ResetCounters = true
	ConfigYAML.PersistCountKeys = 0
	ConfigYAML.StatsPrefix = statsPrefixName
	ConfigYAML.StoreDb = dbPath
	ConfigYAML.Prefix = ""
	ConfigYAML.ExtraTags = ""
	ConfigYAML.PercentThreshold = Percentiles{}
	// Percentiles{{Float: 50.0, Str: "50"}, {Float: 80.0, Str: "80"}, {Float: 90.0, Str: "90"}, {Float: 95.0, Str: "95"}}
	ConfigYAML.PrintConfig = false
	ConfigYAML.LogName = "stdout"
	ConfigYAML.LogToSyslog = true
	ConfigYAML.SyslogUDPAddress = "localhost:514"

	Config = ConfigYAML

	os.Setenv("CONFIGOR_ENV_PREFIX", "SD")

	configFile = flag.String("config", "", "Configuration file name (warning not error if not exists). Standard: "+configPath)
	flag.StringVar(&Config.UDPServiceAddress, "udp-addr", ConfigYAML.UDPServiceAddress, "UDP listen service address")
	flag.StringVar(&Config.TCPServiceAddress, "tcp-addr", ConfigYAML.TCPServiceAddress, "TCP listen service address, if set")
	flag.Int64Var(&Config.MaxUDPPacketSize, "max-udp-packet-size", ConfigYAML.MaxUDPPacketSize, "Maximum UDP packet size")
	flag.StringVar(&Config.BackendType, "backend-type", ConfigYAML.BackendType, "MANDATORY: Backend to use: graphite, opentsdb, external, dummy")
	flag.StringVar(&Config.PostFlushCmd, "post-flush-cmd", ConfigYAML.PostFlushCmd, "Command to run on each flush")
	flag.StringVar(&Config.GraphiteAddress, "graphite", ConfigYAML.GraphiteAddress, "Graphite service address")
	flag.StringVar(&Config.OpenTSDBAddress, "opentsdb", ConfigYAML.OpenTSDBAddress, "OpenTSDB service address")
	flag.Int64Var(&Config.FlushInterval, "flush-interval", ConfigYAML.FlushInterval, "Flush interval (seconds)")
	flag.StringVar(&Config.LogLevel, "log-level", ConfigYAML.LogLevel, "Set log level (debug,info,warn,error,fatal)")
	flag.BoolVar(&Config.ShowVersion, "version", ConfigYAML.ShowVersion, "Print version string")
	flag.BoolVar(&Config.DeleteGauges, "delete-gauges", ConfigYAML.DeleteGauges, "Don't send values to graphite for inactive gauges, as opposed to sending the previous value")
	flag.BoolVar(&Config.ResetCounters, "reset-counters", ConfigYAML.ResetCounters, "Reset counters after sending value to backend (send rate) or  send cumulated value (artificial counter - eg. for OpenTSDB & Grafana)")
	flag.Int64Var(&Config.PersistCountKeys, "persist-count-keys", ConfigYAML.PersistCountKeys, "Number of flush-intervals to persist count keys")
	flag.StringVar(&Config.StatsPrefix, "stats-prefix", ConfigYAML.StatsPrefix, "Name for internal application metrics (no prefix prepended)")
	flag.StringVar(&Config.StoreDb, "store-db", ConfigYAML.StoreDb, "Name of database for permanent counters storage (for conversion from rate to counter)")
	flag.StringVar(&Config.Prefix, "prefix", ConfigYAML.Prefix, "Prefix for all stats")
	flag.StringVar(&Config.ExtraTags, "extra-tags", ConfigYAML.ExtraTags, "Default tags added to all measures in format: tag1=value1 tag2=value2")
	flag.Var(&Config.PercentThreshold, "percent-threshold", "Percentile calculation for timers (0-100, may be given multiple times)")
	flag.BoolVar(&Config.PrintConfig, "print-config", ConfigYAML.PrintConfig, "Print config in YAML format")
	flag.StringVar(&Config.LogName, "log-name", ConfigYAML.LogName, "Name of file to log into. If \"stdout\" than logs to stdout.If empty logs go to /dev/null")
	flag.BoolVar(&Config.LogToSyslog, "log-to-syslopg", ConfigYAML.LogToSyslog, "Log to syslog")
	flag.StringVar(&Config.SyslogUDPAddress, "syslog-udp-address", ConfigYAML.SyslogUDPAddress, "Syslog address with port number eg. localhost:514. If empty log to unix socket")
	if parse {
		flag.Parse()
	}

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

				// save 2 flags
				tmpConfig := Config
				// Overwites flags
				Config = ConfigYAML
				// restore 2 flags
				Config.ShowVersion = tmpConfig.ShowVersion
				Config.PrintConfig = tmpConfig.PrintConfig

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

	// Normalize prefix
	Config.Prefix = normalizeDot(Config.Prefix, true)

	// Normalize internal metrics name
	Config.StatsPrefix = normalizeDot(Config.StatsPrefix, true)

	// calculate extraFlags hash
	Config.ExtraTagsHash, err = parseExtraTags(Config.ExtraTags)
	if err != nil {
		fmt.Printf("Extra Tags: \"%s\" - %s\n", Config.ExtraTags, err)
		os.Exit(1)
	}

	// Set InternalLogLevel
	Config.InternalLogLevel, err = log.ParseLevel(Config.LogLevel)
	if err != nil {
		fmt.Printf("Invalid log level: \"%s\"\n", Config.LogLevel)
		os.Exit(1)
	}

}

// Global var used for all metrics
var (
	In             = make(chan *Packet, maxUnprocessedPackets)
	counters       = make(map[string]int64)
	gauges         = make(map[string]float64)
	lastGaugeValue = make(map[string]float64)
	// lastGaugeTags   = make(map[string]map[string]string)
	timers          = make(map[string]Float64Slice)
	countInactivity = make(map[string]int64)
	sets            = make(map[string][]string)
	keys            = make(map[string][]string)
	// tags            = make(map[string]map[string]string)
	dbHandle *bolt.DB
	logFile  io.Writer
)

func main() {
	var err error

	readConfig(true)
	err = validateConfig()
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

	log.SetLevel(Config.InternalLogLevel)

	if Config.LogName == "stdout" {
		log.SetOutput(os.Stdout)
	} else {
		if len(Config.LogName) > 0 {
			logFile, err := os.OpenFile(Config.LogName, os.O_WRONLY|os.O_CREATE, 0640)
			if err != nil {
				fmt.Printf("Error opennig log file: %s\n", err)
				os.Exit(1)
			}
			log.SetFormatter(&log.TextFormatter{DisableColors: true})
			log.SetOutput(logFile)
			defer logFile.Close()
		}
	}

	if Config.LogToSyslog {
		// set syslog
		syslogNetwork := ""
		if len(Config.SyslogUDPAddress) > 0 {
			syslogNetwork = "udp"
		}
		hook, err := logrus_syslog.NewSyslogHook(syslogNetwork, Config.SyslogUDPAddress, syslog.LOG_DEBUG|syslog.LOG_LOCAL3, "statsdaemon")
		if err != nil {
			fmt.Printf("Unable to connect to syslog daemon: %s\n", err)
		} else {
			log.AddHook(hook)
			log.SetFormatter(&log.JSONFormatter{})
		}
	}

	if Config.LogName == "" {
		log.SetOutput(ioutil.Discard)
	}

	// if Config.LogLevel == "debug" {
	// 	go func() {
	// 		log.Println(http.ListenAndServe(":8082", nil))
	// 	}()
	// }

	// Stat
	Stat.Interval = Config.FlushInterval

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
	if Config.BackendType != "external" && Config.BackendType != "graphite" && Config.BackendType != "opentsdb" && Config.BackendType != "dummy" && !doNotCheckBackend {
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
	// err = listener.SetReadBuffer(1024 * 1024 * 50)
	// if err != nil {
	//
	// 	logCtx.WithField("after", "SetReadBuffer").Fatalf("%s", err)
	// }
	//
	// err = listener.SetWriteBuffer(1024 * 1024 * 50)
	// if err != nil {
	//
	// 	logCtx.WithField("after", "SetWriteBuffer").Fatalf("%s", err)
	// }

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
				Stat.ErrorIncr()
			}
			return
		case <-ticker.C:
			if err := submit(time.Now().Add(period), Config.BackendType); err != nil {
				logCtx.Errorf("%s", err)
				Stat.ErrorIncr()
			}
		case s := <-In:
			packetHandler(s)
		}
	}
}
