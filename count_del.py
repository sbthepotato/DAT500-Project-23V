from mrjob.job import MRJob

class MRCountSum(MRJob):

	carrier = ''
	fl_nr = ''
	delay = 0

	def make_runner(self):
		line = line.strip()
		if line.find('OP_UNIQUE_CARRIER') == 0:
			line.split()
			carrier = line[1]
		if line.find('OP_CARRIER_FL_NUM') == 0:
			line.split()
			fl_nr = line[1]
		if line.find('DEP_DELAY_NEW') == 0:
			line.split()
			yield carrier + ' ' + fl_nr, line[1]

	def combiner(self, key, values):
		yield key, sum(values)

	def reducer(self, key, values):
		yield key, sum(values)


if __name__ == '__main__':
    MRCountSum.run()