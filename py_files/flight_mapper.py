#!/usr/bin/env python3

import sys

row = []

for line in sys.stdin:
  line = line.strip().replace(',','').split()

  if line[0] == "LATE_AIRCRAFT_DELAY":
    try:
      data = line[1]
    except IndexError:
      data = ' '
    row.append(data)
    print(','.join(row))
    row = []
  else:
    try:
      data = ' '.join(line[1:])
    except IndexError:
      data = ' '
    row.append(data)