#!/usr/bin/env python3

import sys
import pyspark
from flight_schemas import *
from pyspark.sql.functions import *
from delta import *
from pyspark.sql import Window

if len(sys.argv) > 1:
  print('opening the hdfs://namenode:9000/csv/'+sys.argv[1]+'.csv file')
else:
  sys.exit()

builder = pyspark.sql.SparkSession.builder.appName('flight_count_sortmerge')

spark = configure_spark_with_delta_pip(builder).getOrCreate()


flight_data = spark.read.csv('hdfs://namenode:9000/csv/'+sys.argv[1]+'.csv', schema=flightSchema)\
	.withColumn('FL_DATE', to_date(to_timestamp('FL_DATE', 'M/d/yyyy h:mm:ss a')))

# select relevant columns, dropping the rest
flight_data = flight_data.select( 'year'
																, 'month'
																, 'fl_date'
																, 'op_unique_carrier'
																, 'origin_airport_id'
																, 'dest_airport_id'
																, 'dep_delay_new'
																, 'arr_delay_new'
																, 'cancelled'
																, 'diverted'
																, 'air_time')

# clean dataset from useless columns
flight_data = flight_data.na.drop(subset=['year', 'origin_airport_id', 'dest_airport_id', 'fl_date'])
flight_data = flight_data.fillna({'arr_delay_new': 0.0})

# grab the fl_date of the flight with the highest delay for a given group
windowSpec = Window.partitionBy('year'
															, 'month'
															, 'op_unique_carrier'
															, 'origin_airport_id'
															, 'dest_airport_id').orderBy(col('arr_delay_new').desc())

arr_delay_dates = flight_data.withColumn(
		'rank'
	, rank().over(windowSpec)
).filter(
	col('rank') == 1
).groupBy(
		'year'
	, 'month'
	, 'op_unique_carrier'
	, 'origin_airport_id'
	, 'dest_airport_id'
).agg(
		round(max('arr_delay_new'), 2).alias('max_arr_delay')
	, first('fl_date').alias('max_arr_delay_fl_date')
)

# the rest of the groupby
flight_data = flight_data.groupBy('year'
																, 'month'
																, 'op_unique_carrier'
																, 'origin_airport_id'
																, 'dest_airport_id').agg( round(avg('arr_delay_new'), 2).alias('avg_arr_delay')
																												, round(percentile_approx('arr_delay_new', 0.5), 2).alias('med_arr_delay')
																												, round(avg(col('dep_delay_new') - col('arr_delay_new')), 2).alias('avg_time_recovered')
																												, sum('diverted').alias('nr_diverted')
																												, round(avg('air_time'), 2).alias('avg_airtime')
																												, count('*').alias('flight_count')
																												, sum('cancelled').alias('nr_cancelled'))

# sort the tables for sort merge join
arr_delay_dates = arr_delay_dates.sort(['year', 'month', 'op_unique_carrier', 'origin_airport_id', 'dest_airport_id'])
flight_data = flight_data.sort(['year', 'month', 'op_unique_carrier', 'origin_airport_id', 'dest_airport_id'])

# join the highest delay with the res of the group
flight_data = arr_delay_dates.join( flight_data
																	, on=['year', 'month', 'op_unique_carrier', 'origin_airport_id', 'dest_airport_id']
																	, how='left')

# read the airports and airlines lookup tables
airports = spark.read.csv('hdfs://namenode:9000/lookup_tables/airport_id.csv', schema=numIdSchema)
carriers = spark.read.csv('hdfs://namenode:9000/lookup_tables/unique_carrier.csv', schema=StringIdSchema)

# sort the tables for sort merge join
flight_data = flight_data.sort('op_unique_carrier')
carriers = carriers.sort('id')

flight_data = flight_data.join(
		carriers.select('id', col('val').alias('airline'))
	, flight_data['op_unique_carrier'] == carriers['id']
	, how="left"
)

# sort the tables for sort merge join
flight_data = flight_data.sort('origin_airport_id')
airports = airports.sort('id')

flight_data = flight_data.join(
		airports.select('id', col('val').alias('origin_airport'))
	, flight_data['origin_airport_id'] == airports['id']
	, how="left"
)

# sort the tables for sort merge join
airports_alias = airports.alias('airports_alias')
flight_data = flight_data.sort('dest_airport_id')
airports_alias = airports_alias.sort('id')

flight_data = flight_data.join(
		airports_alias.select('id', col('val').alias('dest_airport'))
	, flight_data['dest_airport_id'] == airports_alias['id']
	, how="left"
)

#flight_data.select('*').where(flight_data.flight_count > 100).show(30, truncate=True)
flight_data.count()
