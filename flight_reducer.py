#!/usr/bin/env python3

from operator import itemgetter
import sys

current_flight = None
current_count = 0
flight = None

for line in sys.stdin: # read input from STDIN
  line = line.strip() # remove leading and trailing whitespace
  try:
    flight, count = line.split('\t', 1) # parse the input we got from email_count_mapper.py, two words, tab delimited
  except:
    print ("error: ", line)
    continue
  
  try: # convert count from str to int, if not possible discard this input
    count = int(count)
  except ValueError:
    continue
  
  if current_flight == flight: #this works because mapper output is sorted by key (flight) by Hadoop before feeding to reducer input
    current_count += count #if email domain hasn't changed continue counting its occurances
  else:
    if current_flight: #if email domain has changed output count for previous email domain to STDOUT and start counting for new email domain
      print ('%s\t%s' % (current_flight, current_count))
    current_count = count
    current_flight = flight

# output the last email domain
if current_flight == flight:
  print ('%s\t%s' % (current_flight, current_count))
