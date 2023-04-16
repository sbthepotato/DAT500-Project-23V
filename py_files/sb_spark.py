#!/usr/bin/env python3

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from delta import *
from pyspark.sql import Window

builder = pyspark.sql.SparkSession.builder.appName("sb_test") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


flightSchema = StructType() \
	.add("YEAR", "integer")\
	.add("QUARTER", "integer")\
	.add("MONTH", "integer")\
	.add("DAY_OF_MONTH", "integer")\
	.add("DAY_OF_WEEK", "integer")\
	.add("FL_DATE", "string")\
	.add("OP_UNIQUE_CARRIER", "string")\
	.add("TAIL_NUM", "string")\
	.add("OP_CARRIER_FL_NUM", "integer")\
	.add("ORIGIN_AIRPORT_ID", "integer")\
	.add("ORIGIN_AIRPORT_SEQ_ID", "integer")\
	.add("ORIGIN_CITY_MARKET_ID", "integer")\
	.add("ORIGIN", "string")\
	.add("ORIGIN_CITY_NAME", "string")\
	.add("ORIGIN_STATE_NM", "string")\
	.add("DEST_AIRPORT_ID", "integer")\
	.add("DEST_AIRPORT_SEQ_ID", "integer")\
	.add("DEST_CITY_MARKET_ID", "integer")\
	.add("DEST", "string")\
	.add("DEST_CITY_NAME", "string")\
	.add("DEST_STATE_NM", "string")\
	.add("DEP_DELAY", "float")\
	.add("DEP_DELAY_NEW", "float")\
	.add("DEP_DEL15", "float")\
	.add("ARR_DELAY", "float")\
	.add("ARR_DELAY_NEW", "float")\
	.add("ARR_DEL15", "float")\
	.add("CANCELLED", "float")\
	.add("CANCELLATION_CODE", "float")\
	.add("DIVERTED", "float")\
	.add("AIR_TIME", "float")\
	.add("DISTANCE", "float")\
	.add("DISTANCE_GROUP", "float")\
	.add("CARRIER_DELAY", "float")\
	.add("WEATHER_DELAY", "float")\
	.add("NAS_DELAY", "float")\
	.add("SECURITY_DELAY", "float")\
	.add("LATE_AIRCRAFT_DELAY", "float")

numIdSchema = StructType()\
	.add('id', 'integer')\
  .add('val', 'string')

StringIdSchema = StructType()\
	.add('id', 'string')\
  .add('val', 'string')


flight_data=spark.read.csv("hdfs://namenode:9000/csv/2022-12.csv", schema=flightSchema)\
	.withColumn("FL_DATE",to_date(to_timestamp("FL_DATE", "M/d/yyyy h:mm:ss a")))

#flight_data.printSchema()

airports = spark.read.csv('hdfs://namenode:9000/lookup_tables/airport_id.csv', schema=numIdSchema)

carriers = spark.read.csv('hdfs://namenode:9000/lookup_tables/unique_carrier.csv', schema=StringIdSchema)


flight_data = flight_data.na.drop(subset=["year", 'origin_airport_id', 'dest_airport_id', 'fl_date'])
flight_data = flight_data.fillna({'arr_delay_new':0.0})

#flight_data.select(flight_data.columns[:32]).show(1000)

windowSpec = Window.partitionBy(
		"year"
	, "month"
	, "op_unique_carrier"
	, "origin_airport_id"
	, "dest_airport_id"
).orderBy(col("arr_delay_new").desc())

arr_delay_dates = flight_data.withColumn(
		"rank"
  , rank().over(windowSpec)
).filter(
	col("rank") == 1
).groupBy(
			"year"
    , "month"
    , "op_unique_carrier"
    , "origin_airport_id"
    , "dest_airport_id"
).agg(
		round(max("arr_delay_new"), 2).alias("max_arr_delay")
	, first("fl_date").alias("max_arr_delay_fl_date")
)

delay_group = flight_data.groupBy('year'
                                , 'month'
                                , 'op_unique_carrier'
                                , 'origin_airport_id'
																, 'dest_airport_id').agg(round(avg('arr_delay_new'), 2).alias('avg_arr_delay')
																												, round(percentile_approx('arr_delay_new', 0.5), 2).alias("med_arr_delay")
																												, round(avg(col('dep_delay_new') - col('arr_delay_new')), 2).alias('avg_time_recovered')
																												, sum('diverted').alias('nr_diverted')
																												, round(avg('air_time'), 2).alias('avg_airtime')
																												, count('*').alias('flight_count')
                                                        , sum('cancelled').alias('nr_cancelled')
																												#, count(when(col('cancellation_code') == 'A', 1)).alias('code_a_count')
																												#, count(when(col('cancellation_code') == 'B', 1)).alias('code_b_count')
																												#, count(when(col('cancellation_code') == 'C', 1)).alias('code_c_count')
																												#, count(when(col('cancellation_code') == 'D', 1)).alias('code_d_count')
																													)

delay_trends = arr_delay_dates.join(delay_group
                                    , on=["year"
																					, "month"
																					, "op_unique_carrier"
																					, "origin_airport_id"
																					, "dest_airport_id"]
                                    , how="left")

delay_trends = delay_trends.orderBy(desc('nr_cancelled'))

#delay_trends.select('*').where(delay_trends.flight_count > 100).show(100)

delay_trends_with_names = delay_trends.join(carriers.select('id', col('val').alias('airline')), delay_trends['op_unique_carrier'] == carriers['id'], how="left")
delay_trends_with_names = delay_trends_with_names.join(airports.select('id', col('val').alias('origin_airport')), delay_trends_with_names['origin_airport_id'] == airports['id'], how="left")

airports_alias = airports.alias('airports_alias')
delay_trends_with_names = delay_trends_with_names.join(airports_alias.select('id', col('val').alias('dest_airport')), delay_trends_with_names['dest_airport_id'] == airports_alias['id'], how="left")

delay_trends_with_names.select('year'
                              , 'month'
															, 'airline'
                              , 'origin_airport'
                              , 'dest_airport'
                              , 'max_arr_delay'
                              , 'max_arr_delay_fl_date'
                              , 'avg_arr_delay'
                              , 'med_arr_delay'
                              , 'avg_time_recovered'
															, 'nr_diverted'
                              , 'avg_airtime'
                              #, 'code_a_count'
                              #, 'code_b_count'
                              #, 'code_c_count'
                              #, 'code_d_count'
                              , 'flight_count'
                              , 'nr_cancelled').where(delay_trends.flight_count > 100).show(100, truncate=True)
