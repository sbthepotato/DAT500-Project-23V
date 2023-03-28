from mrjob.job import MRJob
import csv
import os

class structure(MRJob):

  def mapper(self, _, line):
    value1, value2 = line.strip().split(' ')
    yield None, (value1, value2)


  def reducer(self, _, values):
    output = csv.writer(open('output.csv', 'w'))
    for value1, value2 in values:
      output.writerow([value1, value2])


if __name__ == '__main__':
    structure.run()