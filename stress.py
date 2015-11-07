import socket
import time
import random
import sys
import statsd

# lib - https://github.com/jsocol/
# alternative lib - https://github.com/WoLpH/python-statsd

## https://github.com/jsocol/pystatsd/blob/master/docs/reference.rst
s = statsd.StatsClient('localhost', 8125, prefix='test.ex')
start = time.time()

while True:
  # incr/decr - liczniki
  for i in range(1, int(10000*random.random())):
        s.incr('foo_incr.^rate=1000')
        s.incr('foo_incr.^rate=0100', rate=0.100)
        s.incr('foo_incr.^rate=0010', rate=0.010)
        s.incr('foo_incr.^rate=0001', rate=0.001)

  # gauge - wartosci (nie sumowalne, np. temperatura)
  s.gauge('foo_gauge.^test=ex2', int(100 * random.random()))

  # timer - czas trwania + histogram
  @s.timer('foo_timer', rate=1)
  def sleep():
        time.sleep(random.random())

  sleep()
