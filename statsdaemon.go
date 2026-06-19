package main

import (
	"fmt"
	"io"
	"log/syslog"

	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"runtime/pprof"
	"strings"

	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"
	logrus_syslog "github.com/sirupsen/logrus/hooks/syslog"
	flag "github.com/spf13/pflag"
	bolt "go.etcd.io/bbolt"
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

	// empty disables the pprof/HTTP debug server
	defaultPprofAddr = ""
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
	PprofAddr        string             `yaml:"pprof-addr"`
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

// setConfigDefaults populates Config with built-in defaults.
func setConfigDefaults() {
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
	Config.PprofAddr = defaultPprofAddr

	// DebugMetrics
	Config.CfgDebugMetrics.Enabled = false
	Config.CfgDebugMetrics.Patterns = []string{}
	Config.CfgDebugMetrics.FileName = defaultDebugMetricFile

	// File backend config
	Config.CfgFileBackend.FileName = defaultFileBackendFile
}

// registerFlags wires command-line flags to the package-level flag vars.
func registerFlags() {
	configFile = flag.String("config", "", "Configuration file name (warning not error if not exists). Standard: "+configPath)
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	showVersion = flag.Bool("version", false, "show program version")
	printConfig = flag.Bool("print-config", false, "print curent config in yaml (can be used as default config)")
}

// loadConfig loads the optional config file (a missing/unreadable file is a
// warning, not an error) and computes derived fields. It returns an error only
// for invalid configuration values the daemon cannot run with.
func loadConfig(configFilePath string) error {
	if len(configFilePath) > 0 {
		if _, err := os.Stat(configFilePath); os.IsNotExist(err) {
			fmt.Printf("# Warning: No config file: %s\n", configFilePath)
		} else if err := configor.Load(&Config, configFilePath); err != nil {
			fmt.Printf("Error loading config file: %s\n", err)
		}
	}

	// Normalize prefix and internal metrics name
	Config.Prefix = normalizeDot(Config.Prefix, true)
	Config.StatsPrefix = normalizeDot(Config.StatsPrefix, true)

	var err error
	if Config.ExtraTagsHash, err = parseExtraTags(Config.ExtraTags); err != nil {
		return fmt.Errorf("extra tags %q: %s", Config.ExtraTags, err)
	}

	if Config.InternalLogLevel, err = log.ParseLevel(Config.LogLevel); err != nil {
		return fmt.Errorf("invalid log level %q", Config.LogLevel)
	}

	Config.ParsedPostFlushCmd = strings.Split(Config.PostFlushCmd, " ")
	return nil
}

// readConfig orchestrates defaults -> flags -> file load -> derived fields.
func readConfig(parse bool) error {
	setConfigDefaults()
	os.Setenv("CONFIGOR_ENV_PREFIX", "SD")
	registerFlags()
	if parse {
		flag.Parse()
	}
	return loadConfig(*configFile)
}

// Global var used for all metrics
var (
	In = make(chan *Packet, maxUnprocessedPackets)

	// current holds the accumulation maps for the in-progress flush interval.
	// It is owned by the monitor goroutine and swapped at each flush.
	current = newMetrics()

	// Persistent flush-side state, touched only by the flush path.
	lastGaugeValue  = make(map[string]float64)
	countInactivity = make(map[string]int64)

	dbHandle *bolt.DB
	logFile  io.Writer
)

func main() {
	if err := readConfig(true); err != nil {
		fmt.Printf("Config error: %s\n", err)
		os.Exit(1)
	}

	if err := validateConfig(); err != nil {
		fmt.Printf("\n%s\n\n", err)
		//flag.Usage()
		os.Exit(1)
	}

	var err error

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
		log.SetOutput(io.Discard)
	}

	// Optional pprof/HTTP debug server (net/http/pprof registers its handlers
	// on the default mux via the blank import above).
	if Config.PprofAddr != "" {
		go func() {
			plog := log.WithFields(log.Fields{"in": "pprof"})
			plog.Infof("Serving pprof/HTTP debug server on %s", Config.PprofAddr)
			if err := http.ListenAndServe(Config.PprofAddr, nil); err != nil {
				plog.Errorf("pprof server stopped: %s", err)
			}
		}()
	}

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
			if err := submit(current, time.Now().Add(period)); err != nil {
				logCtx.Errorf("%s", err)
				Stat.OtherErrorsInc()
			}
			return
		case <-ticker.C:
			if err := submit(current, time.Now().Add(period)); err != nil {
				logCtx.Errorf("%s", err)
				Stat.OtherErrorsInc()
			}
		case s := <-In:
			current.handlePacket(s)
		}
	}
}
