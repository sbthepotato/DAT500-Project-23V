#!/usr/bin/env python3

import sys

current_carrier = None
current_delay = 0.0
current_count = 0
carrier = None

for line in sys.stdin:
  line = line.strip()
  try:
    carrier, delay = line.split('\t', 1)
  except Exception as e:
    if carrier:
      delay = 0.0
    else:
      print ("error: ", line, e)
      continue
  
  try:
    delay = float(delay)
  except ValueError:
    continue

  if current_carrier == carrier:
    current_delay += delay 
    current_count += 1
  else:
    if current_carrier:
      print ('%s\t%s\t%s' % (current_carrier, current_delay, current_count))
    current_delay = delay
    current_carrier = carrier
    current_count = 1

if current_carrier == carrier:
  print ('%s\t%s\t%s' % (current_carrier, current_delay, current_count))
