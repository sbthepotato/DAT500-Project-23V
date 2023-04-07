#!/usr/bin/env python3

import sys

for line in sys.stdin:
  line = line.split(',')
  try:
    print('%s\t%s' % (line[6], line[22]))
  except IndexError:
    continue

