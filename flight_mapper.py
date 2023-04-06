#!/usr/bin/env python3

import sys


for line in sys.stdin:
  line = line.strip().replace(',','').split()
  if line[0] == 'OP_UNIQUE_CARRIER':
    print ('%s\t%s' % (line[1], 1)) 