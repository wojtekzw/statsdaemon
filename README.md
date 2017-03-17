statsdaemon
==========
[![CircleCI](https://circleci.com/gh/wojtekzw/statsdaemon.svg?style=svg)](https://circleci.com/gh/wojtekzw/statsdaemon)


Port of Etsy's statsd server (https://github.com/etsy/statsd), written in Go (based on
  [szaydel/statsdaemon](https://github.com/szaydel/statsdaemon) and changes from
  [alexclear/statsdaemon](https://github.com/alexclear/statsdaemon) and others)
  with many enhancements (e.g. floats in timers and gauges, sets, key/values, absolute counters, many different backends)

Supports
--------
Metrics types:
* Timers (with optional percentiles)
* Counters (positive and negative with optional sampling) + ability send counter as rate(per flush interval - default) or absolute counter (using local BoltDB)
* Gauges (including relative operations)
* Sets
* Key/values (unique untyped)

Float numbers are supported in Timers and  Gauges

Backends supported:
* Graphite
* External shell command (data on STDIN) or output to STDOUT (when no external command provided)
* OpenTSDB
* File - enables to send metrics directly to specified file

Other:
* Read configuration from YAML file
* Ability to save configuration to YAML file
* UDP and TCP listeners
* Internal statistics is sent to datastore - for performance monitoring (can be switched off)
* Ablility to enable  Golang CPU profiling using command line switch 

```
Tag are supported as encoded in bucket name eg:
cpu.load.idle.^host=dev.^env=prod.^zone=west

It means:
  gauge name: cpu.load.idle
  tags: host = dev, env = prod, zone = west
```
Tags encoding pattern can be changed/enhanced in function `parseBucketAndTags(name string) (string, map[string]string, error)`


Installing
==========
### Build from source
```
go get github.com/wojtekzw/statsdaemon
cd statsdaemon
go build
```

### Build Debian Linux package using Docker

```
docker build -t go-deb-builder .
docker run --rm -v "$PWD:/app" go-deb-builder deb-build.sh
```


Command Line Options
====================

```
Usage of ./statsdaemon:
   --config string
         Configuration file name (warning not error if not exists). Standard: /etc/statsdaemon/statsdaemon.yml
   --cpuprofile string
         write cpu profile to file
   --print-config
         print curent config in yaml (can be used as default config)
   --version
         show program version
```


YAML config file
===================
```
# Default config file in YAML

#config format version - curently 1 
cfg-format: 1

# UDP & TCP can be used at the same time
# UDP listening address and port
udp-addr: :8125

# TCP listening address and port
tcp-addr: ""

# maximum size of UDP packet that can be received
max-udp-packet-size: 1432

# backend types: 
# - external - send metrics to stdin of command specified on 'post-flush-cmd'
# - file - send metrics to 'file-name' in 'file-backend'
# - graphite - send metrics to graphite at address specified in 'graphite'
# - opentdsb - send metrics to opentdsb at address specified in 'opentdsb'
# - dummy - do nothing backend
#
# backend-type - one of the above backend types
backend-type: file
file-backend:
  file-name: /tmp/statsdaemon_metrics.log
# command to run (with args) or stdout. Shell redirects like <>| don't work here  
post-flush-cmd: stdout
graphite: 127.0.0.1:2003
opentsdb: 127.0.0.1:4242

# time in seconds to flush agregated metrics to backend
flush-interval: 10

# log levels: fatal,error,warn,info,debug
log-level: error

# delete gauge metrics if there is no new data or send last gauge value 
delete-gauges: true

# reset counter metrics to 0 after each flush or keep them growing using 'store-db' to keep value between restarts 
reset-counters: true

# number of flush-intervals to persist count keys
persist-count-keys: 0

# prefix for internal application metrics
stats-prefix: statsdaemon.

# name of database for permanent counters storage
store-db: /tmp/statsdaemon.db

# prefix for all stats (except internal application metrics)
prefix: ""

# default tags added to all measures in format: tag1=value1 tag2=value2
extra-tags: ""

# timers percentiles eg.:
#percent-threshold:  
#- value: 50
#  name: "50"
#- value: 80
#  name: "80"
#- value: 90
#  name: "90"
#- value: 95
#  name: "95"
percent-threshold: []

# log destination
log-name: stdout

# log to syslog (addtionaly to the destination in 'log-name')
log-to-syslog: true

# syslog udp address or unix socket if empty 
# syslog-udp-address: localhost:514
syslog-udp-address: ""

# disable sending internal application stats do backend
disable-stat-send: false

```