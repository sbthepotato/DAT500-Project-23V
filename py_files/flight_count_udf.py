#!/usr/bin/env python3

import pyspark
from flight_schemas import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *
from pyspark.sql import Window
from pyspark.sql.types import FloatType, BooleanType

builder = pyspark.sql.SparkSession.builder.appName('flight_count_udf')
# .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
# .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def replace_null(value, default):
  if value is None:
    return default
  return value

def drop_null(*cols):
  for col in cols:
    if col is None:
      return False
  return True

replace_null_udf = udf(lambda value, default: replace_null(value, default), FloatType())
drop_null_udf = udf(lambda *cols: drop_null(*cols), BooleanType())

flight_data = spark.read.csv('hdfs://namenode:9000/csv/2020.csv', schema=flightSchema)\
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
flight_data = flight_data.filter(drop_null_udf(*[col(c) for c in ['year', 'origin_airport_id', 'dest_airport_id', 'fl_date']]))
flight_data = flight_data.withColumn('arr_delay_new', replace_null_udf(col('arr_delay_new'), lit(0.0)))

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

# join the highest delay with the res of the group
flight_data = arr_delay_dates.join( flight_data
																	, on=['year', 'month', 'op_unique_carrier', 'origin_airport_id', 'dest_airport_id']
																	, how='left')

# read the airports and airlines lookup tables
airports = spark.read.csv('hdfs://namenode:9000/lookup_tables/airport_id.csv', schema=numIdSchema)
carriers = spark.read.csv('hdfs://namenode:9000/lookup_tables/unique_carrier.csv', schema=StringIdSchema)


flight_data = flight_data.join(
		carriers.select('id', col('val').alias('airline'))
	, flight_data['op_unique_carrier'] == carriers['id']
	, how="left"
)

flight_data = flight_data.join(
		airports.select('id', col('val').alias('origin_airport'))
	, flight_data['origin_airport_id'] == airports['id']
	, how="left"
)

airports_alias = airports.alias('airports_alias')
flight_data = flight_data.join(
		airports_alias.select('id', col('val').alias('dest_airport'))
	, flight_data['dest_airport_id'] == airports_alias['id']
	, how="left"
)

flight_data.select('year', 'month', 'airline', 'origin_airport', 'dest_airport', 'max_arr_delay', 'max_arr_delay_fl_date', 'avg_arr_delay', 'med_arr_delay', 'avg_time_recovered', 'nr_diverted', 'avg_airtime', 'flight_count', 'nr_cancelled').where(flight_data.flight_count > 100).show(100, truncate=True)
