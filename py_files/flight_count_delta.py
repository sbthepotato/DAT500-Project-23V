#!/usr/bin/env python3

import sys
from flight_schemas import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *
from pyspark.sql import Window

if len(sys.argv) > 1:
  print('opening the hdfs://namenode:9000/csv/'+sys.argv[1]+'.csv file')
else:
  sys.exit()

builder = SparkSession.builder.appName('flight_count_delta')
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

# join the highest delay with the res of the group
flight_data = arr_delay_dates.join( 
    broadcast(flight_data)
	, on=['year', 'month', 'op_unique_carrier', 'origin_airport_id', 'dest_airport_id']
	, how='left')

# read the airports and airlines lookup tables
airports = spark.read.csv('hdfs://namenode:9000/lookup_tables/airport_id.csv', schema=numIdSchema)
carriers = spark.read.csv('hdfs://namenode:9000/lookup_tables/unique_carrier.csv', schema=StringIdSchema)


flight_data = flight_data.join( 
		broadcast(carriers.select('id', col('val').alias('airline')))
	, flight_data['op_unique_carrier'] == carriers['id']
	, how='left')

flight_data = flight_data.join(	
		broadcast(airports.select('id', col('val').alias('origin_airport')))
	, flight_data['origin_airport_id'] == airports['id']
	, how='left')

airports_alias = airports.alias('airports_alias')
flight_data = flight_data.join( 
    broadcast(airports_alias.select('id', col('val').alias('dest_airport')))
	, flight_data['dest_airport_id'] == airports_alias['id']
	, how='left')

flight_data = flight_data.drop("id")

#flight_data.select('*').where(flight_data.flight_count > 100).show(100, truncate=True)

# Storing in Delta Table
if DeltaTable.isDeltaTable(spark, "hdfs://namenode:9000/spark-warehouse/flight_data_table"):
  # Perform the upsert operation
  deltaDF = DeltaTable.forPath(spark, "hdfs://namenode:9000/spark-warehouse/flight_data_table")
  merge_condition = "existing.year = upsert.year \
                    AND existing.month = upsert.month \
                    AND existing.op_unique_carrier = upsert.op_unique_carrier \
                    AND existing.origin_airport_id = upsert.origin_airport_id \
                    AND existing.dest_airport_id = upsert.dest_airport_id "

  deltaDF.alias('existing') \
        .merge(flight_data.alias('upsert'), merge_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
  print("Delta Table called 'flight_data_table' updated.")  
else:
  # Create new delta table
  flight_data.write.format("delta").mode("overwrite").saveAsTable("flight_data_table")
  print("Delta Table called 'flight_data_table' created.")
