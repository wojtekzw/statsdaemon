statsdaemon
==========

Port of Etsy's statsd server (https://github.com/etsy/statsd), written in Go (based on
  [szaydel/statsdaemon](https://github.com/szaydel/statsdaemon) and changes from
  [alexclear/statsdaemon](https://github.com/alexclear/statsdaemon) and others)
  with many enhancements (e.g. floats in timers and gauges, sets, key/values, absolute counters, many different backends)

Supports

Metrics types
* Timers (with optional percentiles)
* Counters (positive and negative with optional sampling) + ability send counter as rate(default) or absolute counter (using local BoltDB)
* Gauges (including relative operations)
* Sets
* Key/values (unique untyped)

Float numbers are supported in Timers and  Gauges

Backend supported
* Graphite
* External shell command (data on STDIN) or output to STDOUT (when no external command provided)
* OpenTSDB

Others
* Read configuration from YAML file
* Ability to save configuration to YAML file
* UDP and TCP listeners

```
Tag are supported as encoded in bucket name eg:
cpu.load.idle.^host=dev.^env=prod.^zone=west

It means:
  gauge name: cpu.load.idle
  tags: host = dev, env = prod, zone = west
```
Tags encoding pattern can be changed/enchanced in function `parseBucketAndTags(name string) (string, map[string]string, error)`

Importants bugs:
* reading config from YAML overwrites config from flags - USE ONLY ONE METHOD NOW !
** eg. statsdaemon --config="myconfig.yml" is OK
** statsdaemon --config="myconfig.yml" --debug=true is not OK as debug will have default value or from myconfig.yml if exists in config file


Installing
==========
### Building from Source
```
go get github.com/wojtekzw/statsdaemon
cd statsdaemon
go build
```


Command Line Options
====================

```
Usage of ./statsdaemon:
      --backend-type="external": MANDATORY: Backend to use: graphite, opentsdb, external
      --config="/etc/statsdaemon/statsdaemon.yml": Configuration file name (warning not error if not exists)
      --debug=false: Print statistics sent to backend
      --delete-gauges=true: Don't send values to graphite for inactive gauges, as opposed to sending the previous value
      --extra-tags="": Default tags added to all measures in format: tag1=value1,tag2=value2
      --flush-interval=10: Flush interval (seconds)
      --graphite="127.0.0.1:2003": Graphite service address
      --log-name="stdout": Name of file to log into. If empty or "stdout" than logs to stdout
      --max-udp-packet-size=1472: Maximum UDP packet size
      --opentsdb="127.0.0.1:4242": OpenTSDB service address
      --persist-count-keys=0: Number of flush-intervals to persist count keys
      --post-flush-cmd="stdout": Command to run on each flush
      --postfix="": Postfix for all stats
      --prefix="": Prefix for all stats
      --print-config=false: Print config in YAML format
      --receive-counter="statsdaemon.metrics.count": Metric name for total metrics received per interval (no prefix,postfix added, only extra-tags)
      --reset-counters=true: Reset counters after sending value to backend (send rate) or  send cumulated value (artificial counter - eg. for OpenTSDB & Grafana)
      --store-db="/tmp/statsdaemon.db": Name of database for permanent counters storage (for conversion from rate to counter)
      --tcp-addr="": TCP listen service address, if set
      --udp-addr=":8125": UDP listen service address
      --version=false: Print version string

```
