import socket
import time
import random
import sys
import statsd

# lib - https://github.com/jsocol/
# alternative lib - https://github.com/WoLpH/python-statsd

## https://github.com/jsocol/pystatsd/blob/master/docs/reference.rst
s = statsd.StatsClient('localhost', 8125, prefix='')
#s = statsd.TCPStatsClient('localhost', 8125, prefix='')
start = time.time()

k = 0
MaxLoop = 3
while True:
  # incr/decr - liczniki
  s.incr('foo_incr.^rate=1000')
  s.incr('foo_incr.^rate=0100', rate=0.100)
  s.incr('foo_incr.^rate=0010', rate=0.010)
  s.incr('foo_incr.^rate=0001', rate=0.001)
  s.incr('foo_incr', rate=0.1)

  # gauge - wartosci (nie sumowalne, np. temperatura)
  s.gauge('foo_gauge1.^test=wojtekz', int(100 * random.random()))
  s.gauge('foo_gauge2.^test=wojtekz', int(100 * random.random()))

  # timer - czas trwania + histogram
  @s.timer('foo_timer', rate=1)
  @s.timer('foo_timer.^test=ala', rate=0.1)
  def sleep():
        time.sleep(random.random())

  sleep()
  k = k + 1
