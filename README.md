statsdaemon
==========

Port of Etsy's statsd (https://github.com/etsy/statsd), written in Go (based on
  [szaydel/statsdaemon](https://github.com/szaydel/statsdaemon) )

Supports

* Timing (with optional percentiles)
* Counters (positive and negative with optional sampling)
* Gauges (including relative operations)
* Sets
* Key/values (unique untyped)

Note: Only integers are supported for metric values.

Backend supported
* Graphite (not working now)
* External shell command (data on STDIN) or output to STDOUT (when no external command provided)



Installing
==========
### Building from Source
```
git clone https://github.com/wojtekzw/statsdaemon
cd statsdaemon
go get github.com/bmizerany/assert #for tests
go build
```


Command Line Options
====================

```
Usage of statsdaemon:
  -address string
        UDP service address (default ":8125")
  -debug
        print statistics sent to graphite
  -delete-gauges
        don't send values to graphite for inactive gauges, as opposed to sending the previous value (default true)
  -flush-interval int
        Flush interval (seconds) (default 10)
  -graphite string
        Graphite service address (or - to disable) (default "127.0.0.1:2003")
  -max-udp-packet-size int
        Maximum UDP packet size (default 1472)
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
  -tcpaddr string
        TCP service address, if set
  -version
        print version string
```
