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

	"runtime/pprof"
	"strings"

	log "github.com/Sirupsen/logrus"
	logrus_syslog "github.com/Sirupsen/logrus/hooks/syslog"
	"github.com/boltdb/bolt"
	"github.com/jinzhu/configor"
	flag "github.com/ogier/pflag"
	"gopkg.in/yaml.v2"
)

// Network constants & dbName

const (
	defaultCfgFormat      = 1
	maxUnprocessedPackets = 100000

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

	defaultFileBackendFile = ""
	defaultDebugMetricFile = ""
)

// ConfigFileBackend - file backend config.
type ConfigFileBackend struct {
	FileName string `yaml:"file-name"`
	// private below
	LogFile *os.File `yaml:"-" ignore:"true"`
}

// ConfigDebugMetrics - debug metrics config.
type ConfigDebugMetrics struct {
	Enabled  bool     `yaml:"enabled"`
	Patterns []string `yaml:"patterns"`
	FileName string   `yaml:"file-name"`
	// private below
	LogFile *os.File `yaml:"-" ignore:"true"`
}

// ConfigApp - apppliaction config.
type ConfigApp struct {
	CfgFormat         int    `yaml:"cfg-format"`
	UDPServiceAddress string `yaml:"udp-addr"`
	TCPServiceAddress string `yaml:"tcp-addr"`
	MaxUDPPacketSize  int64  `yaml:"max-udp-packet-size"`
	BackendType       string `yaml:"backend-type"`
	//Backends          []string          `yaml:"backends"`
	CfgFileBackend   ConfigFileBackend  `yaml:"file-backend"`
	PostFlushCmd     string             `yaml:"post-flush-cmd"`
	GraphiteAddress  string             `yaml:"graphite"`
	OpenTSDBAddress  string             `yaml:"opentsdb"`
	FlushInterval    int64              `yaml:"flush-interval"`
	LogLevel         string             `yaml:"log-level"`
	DeleteGauges     bool               `yaml:"delete-gauges"`
	ResetCounters    bool               `yaml:"reset-counters"`
	PersistCountKeys int64              `yaml:"persist-count-keys"`
	StatsPrefix      string             `yaml:"stats-prefix"`
	StoreDb          string             `yaml:"store-db"`
	Prefix           string             `yaml:"prefix"`
	ExtraTags        string             `yaml:"extra-tags"`
	PercentThreshold Percentiles        `yaml:"percent-threshold"`
	LogName          string             `yaml:"log-name"`
	LogToSyslog      bool               `yaml:"log-to-syslog"`
	SyslogUDPAddress string             `yaml:"syslog-udp-address"`
	DisableStatSend  bool               `yaml:"disable-stat-send"`
	CfgDebugMetrics  ConfigDebugMetrics `yaml:"debug-metrics"`

	// private - calculated below
	ExtraTagsHash      map[string]string `yaml:"-"`
	InternalLogLevel   log.Level         `yaml:"-"`
	ParsedPostFlushCmd []string          `yaml:"-"`
}

// Global vars for command line flags
var (
	configFile  *string
	cpuprofile  *string
	showVersion *bool
	printConfig *bool
	Config      = ConfigApp{}
	Stat        = DaemonStat{}

	signalchan chan os.Signal // for signal exits
)

func readConfig(parse bool) {
	var err error
	// Set defaults
	Config.CfgFormat = defaultCfgFormat
	Config.UDPServiceAddress = defaultUDPServiceAddress
	Config.TCPServiceAddress = defaultTCPServiceAddress
	Config.MaxUDPPacketSize = maxUDPPacket
	Config.BackendType = defaultBackendType
	Config.PostFlushCmd = "stdout"
	Config.GraphiteAddress = defaultGraphiteAddress
	Config.OpenTSDBAddress = defaultOpenTSDBAddress
	Config.FlushInterval = flushInterval
	Config.LogLevel = "error"
	Config.DeleteGauges = true
	Config.ResetCounters = true
	Config.PersistCountKeys = 0
	Config.StatsPrefix = statsPrefixName
	Config.StoreDb = dbPath
	Config.Prefix = ""
	Config.ExtraTags = ""
	Config.PercentThreshold = Percentiles{}
	Config.LogName = "stdout"
	Config.LogToSyslog = true
	Config.SyslogUDPAddress = ""
	Config.DisableStatSend = false

	// DebugMetrics
	Config.CfgDebugMetrics.Enabled = false
	Config.CfgDebugMetrics.Patterns = []string{}
	Config.CfgDebugMetrics.FileName = defaultDebugMetricFile

	// File backend config
	Config.CfgFileBackend.FileName = defaultFileBackendFile

	os.Setenv("CONFIGOR_ENV_PREFIX", "SD")

	configFile = flag.String("config", "", "Configuration file name (warning not error if not exists). Standard: "+configPath)
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	showVersion = flag.Bool("version", false, "show program version")
	printConfig = flag.Bool("print-config", false, "print curent config in yaml (can be used as default config)")

	if parse {
		flag.Parse()
	}

	if len(*configFile) > 0 {
		if _, err = os.Stat(*configFile); os.IsNotExist(err) {
			fmt.Printf("# Warning: No config file: %s\n", *configFile)
			*configFile = ""
		}

		if len(*configFile) > 0 {
			err = configor.Load(&Config, *configFile)
			if err != nil {
				fmt.Printf("Error loading config file: %s\n", err)
			}
		}

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

	Config.ParsedPostFlushCmd = strings.Split(Config.PostFlushCmd, " ")

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
		//flag.Usage()
		os.Exit(1)
	}

	if Config.CfgFileBackend.LogFile != nil {
		defer Config.CfgFileBackend.LogFile.Close()
	}

	if Config.CfgDebugMetrics.LogFile != nil {
		defer Config.CfgDebugMetrics.LogFile.Close()
	}

	//Profile
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *showVersion {
		fmt.Printf("statsdaemon v%s (built w/%s)\nBuildDate: %s\nGitHash: %s\n", StatsdaemonVersion, runtime.Version(), BuildDate, GitHash)
		os.Exit(0)
	}

	if *printConfig {
		out, _ := yaml.Marshal(Config)
		fmt.Printf("# Default config file in YAML\n%s", string(out))
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
	Stat.Init(In, 200*time.Millisecond, Config.FlushInterval)

	signalchan = make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)

	dbHandle, err = bolt.Open(Config.StoreDb, 0644, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		fmt.Printf("Error opening BoldDB: %v\n", err)
		log.WithFields(log.Fields{
			"in": "main",
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
	doNotCheckBackend := *printConfig || *showVersion
	if Config.BackendType == "" && !doNotCheckBackend {
		return fmt.Errorf("Parameter error: backend-type can't be empty")
	}
	if Config.BackendType != "external" && Config.BackendType != "graphite" && Config.BackendType != "opentsdb" && Config.BackendType != "file" && Config.BackendType != "dummy" && !doNotCheckBackend {
		return fmt.Errorf("Parameter error: Invalid backend-type: %s", Config.BackendType)
	}

	if (Config.GraphiteAddress == "-" || Config.GraphiteAddress == "") && Config.BackendType == "graphite" {
		return fmt.Errorf("Parameter error: Graphite backend selected and no graphite server address")
	}

	if (Config.OpenTSDBAddress == "-" || Config.OpenTSDBAddress == "") && Config.BackendType == "opentsdb" {
		return fmt.Errorf("Parameter error: OpenTSDB backend selected and no OpenTSDB server address")
	}

	if Config.BackendType == "file" {
		if len(Config.CfgFileBackend.FileName) == 0 {
			return fmt.Errorf("Parameter error: File backend selected and no output FileName")
		}
		f, err := os.OpenFile(Config.CfgFileBackend.FileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return fmt.Errorf("Parameter error: Error openning file %s: %s", Config.CfgFileBackend.FileName, err)
		}
		Config.CfgFileBackend.LogFile = f
	}

	if Config.CfgDebugMetrics.Enabled == true {
		if len(Config.CfgDebugMetrics.FileName) == 0 {
			return fmt.Errorf("Parameter error: Debug matrics enabled and no output FileName")
		}
		f, err := os.OpenFile(Config.CfgDebugMetrics.FileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return fmt.Errorf("Parameter error: Error openning file %s: %s", Config.CfgDebugMetrics.FileName, err)
		}
		Config.CfgDebugMetrics.LogFile = f
	}

	return nil
}

func udpListener() {
	logCtx := log.WithFields(log.Fields{
		"in": "udpListener",
	})

	address, err := net.ResolveUDPAddr("udp", Config.UDPServiceAddress)
	if err != nil {
		fmt.Printf("Error in ResolveUDPAddr: %v\n", err)
		logCtx.Fatalf("%s", err)
	}
	logCtx.Infof("Listening on %s", address)

	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		fmt.Printf("Error in ListenUDP: %v\n", err)
		logCtx.WithField("after", "ListenUDP").Fatalf("%s", err)
	}
	// err = listener.SetReadBuffer(1024 * 1024 * 50)
	// if err != nil {
	//
	// 	logCtx.WithField("after", "SetReadBuffer").Fatalf("%s", err)
	// }
	//

	parseTo(listener, false, In)
}

func tcpListener() {
	logCtx := log.WithFields(log.Fields{
		"in": "tcpListener",
	})
	address, err := net.ResolveTCPAddr("tcp", Config.TCPServiceAddress)
	if err != nil {
		fmt.Printf("Error in ResolveTCPAddr: %v\n", err)
		logCtx.Fatalf("%s", err)
	}
	logCtx.Infof("listening on %s", address)
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		fmt.Printf("Error in ListenTCP: %v\n", err)
		logCtx.Fatalf("%s", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			fmt.Printf("Error in AcceptTCP: %v\n", err)
			logCtx.Fatalf("%s", err)
		}
		go parseTo(conn, true, In)
	}
}

func monitor() {
	logCtx := log.WithFields(log.Fields{
		"in": "monitor",
	})

	period := time.Duration(Config.FlushInterval) * time.Second
	ticker := time.NewTicker(period)
	for {
		select {
		case sig := <-signalchan:
			logCtx.Infof("Caught signal \"%v\"... shutting down", sig)
			if err := submit(time.Now().Add(period), Config.BackendType); err != nil {
				logCtx.Errorf("%s", err)
				Stat.OtherErrorsInc()
			}
			return
		case <-ticker.C:
			if err := submit(time.Now().Add(period), Config.BackendType); err != nil {
				logCtx.Errorf("%s", err)
				Stat.OtherErrorsInc()
			}
		case s := <-In:
			packetHandler(s)
		}
	}
}
