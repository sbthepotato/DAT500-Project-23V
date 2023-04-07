import sys
import csv

def mapper_1(file):
  row = []

  with open(file, 'r') as f:

    for i, line in enumerate(f):
      line = line.strip().replace(',','').split() 

      if line[0] == "LATE_AIRCRAFT_DELAY":
        try:
          data = line[1]
        except IndexError:
          data = ' '
        row.append(data)
        write(row)
        print(','.join(row))
        row = []
      else:
        try:
          data = ' '.join(line[1:])
        except IndexError:
          data = ' '
        row.append(data)
      
      if i == 1000:
        break


def reducer(line):
  carrier = None
  delay = None

  line = line.strip()
  try:
    line = line.split(',')
    carrier = line[6]
    delay = line[22]
  except:
    print ("error: ", line)
    return
  
  print(delay)
  try:
    delay = float(delay)
  except ValueError:
    print(ValueError)
    return
  


def mapper_2(file):
  with open(file, 'r') as f:
    for i, line in enumerate(f):
      line=line.split(',')
      try:
        reducer_2(line[6]+'\t'+line[22])
      except IndexError:
        continue
      if i == 100:
        break

def reducer_2(line):
  line = line.strip()
  try:
    carrier, delay = line.split('\t')

  except Exception as e:
    print ("error: ", line, e)


def write(line):
  with open('../csv/test.csv', 'a') as f:
    writer = csv.writer(f)
    writer.writerow(line)


if __name__ == "__main__":
  #mapper_1('../txt/2022-12.txt')
  mapper_2('../test/test.csv')
