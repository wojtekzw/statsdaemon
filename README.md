statsdaemon
==========

Port of Etsy's statsd server (https://github.com/etsy/statsd), written in Go (based on
  [szaydel/statsdaemon](https://github.com/szaydel/statsdaemon) and changes from
  [alexclear/statsdaemon](https://github.com/alexclear/statsdaemon) and others)
  with many enhacements (e.g. floats in timers and gauges, sets, key/values, absolute counters, many different backends)

Supports

* Timers (with optional percentiles)
* Counters (positive and negative with optional sampling) + ability to keep absolute value by not resetting counter
* Gauges (including relative operations)
* Sets
* Key/values (unique untyped)

Float numbers are supported in Timers and  Gauges

Backend supported
* Graphite
* External shell command (data on STDIN) or output to STDOUT (when no external command provided)
* OpenTSDB

```
Tag are supported as encoded in bucket name eg:
cpu.load.idle.^host=dev.^env=prod.^zone=west

  gauge name: cpu.load.idle
  tags: host = dev, env = prod, zone = west
```
Tags encoding pattern can be changed/enchanced in function `parseBucketAndTags(name string) (string, map[string]string, error)`

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
  -address string
    	UDP service address (default ":8125")
  -backend-type string
    	Backend to use: graphite, opentsdb, external (default "external")
  -debug
    	print statistics sent to backend
  -delete-gauges
    	don't send values to graphite for inactive gauges, as opposed to sending the previous value (default true)
  -flush-interval int
    	Flush interval (seconds) (default 10)
  -graphite string
    	Graphite service address (or - to disable) (default "127.0.0.1:2003")
  -max-udp-packet-size int
    	Maximum UDP packet size (default 1472)
  -opentsdb string
    	openTSDB service address (or - to disable) (default "127.0.0.1:4242")
  -percent-threshold value
    	percentile calculation for timers (0-100, may be given multiple times) (default [])
  -persist-count-keys int
    	number of flush-intervals to persist count keys (default 60)
  -post-flush-cmd string
    	Command to run on each flush (default "stdout")
  -postfix string
    	Postfix for all stats
  -prefix string
    	Prefix for all stats
  -receive-counter string
    	Metric name for total metrics received per interval
  -reset-counters
    	reset counters after sending value to backend or leave current value (eg. for OpenTSDB & Grafana) (default true)
  -tcpaddr string
    	TCP service address, if set
  -version
    	print version string
```
