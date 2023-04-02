import csv
from mrjob.job import MRJob

class TextToCsv(MRJob):
	def mapper(self, _, line):
		# Split the line into key-value pairs.
		key_value_pairs = line.split()

		# Remove any leading/trailing spaces.
		key_value_pairs = [pair.strip() for pair in key_value_pairs]

		# Output the key-value pairs as a single row.
		yield None, key_value_pairs

	def reducer(self, _, rows):
		# Create a CSV writer object.
		writer = csv.writer(open('output.csv', 'w'))

		# Write the header row.
		writer.writerow(['YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK',
										'FL_DATE', 'OP_UNIQUE_CARRIER', 'TAIL_NUM', 'OP_CARRIER_FL_NUM',
										'ORIGIN_AIRPORT_ID', 'ORIGIN_AIRPORT_SEQ_ID', 'ORIGIN_CITY_MARKET_ID',
										'ORIGIN', 'ORIGIN_CITY_NAME', 'ORIGIN_STATE_NM', 'DEST_AIRPORT_ID',
										'DEST_AIRPORT_SEQ_ID', 'DEST_CITY_MARKET_ID', 'DEST', 'DEST_CITY_NAME',
										'DEST_STATE_NM', 'DEP_DELAY', 'DEP_DELAY_NEW', 'DEP_DEL15', 'ARR_DELAY',
										'ARR_DELAY_NEW', 'ARR_DEL15', 'CANCELLED', 'CANCELLATION_CODE',
										'DIVERTED', 'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY',
										'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY'])

		# Write each row to the CSV file.
		for row in rows:
			writer.writerow(row)

if __name__ == '__main__':
    TextToCsv.run()
