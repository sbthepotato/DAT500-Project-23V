#!/usr/bin/env python3

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("US_Flight_Delays") \
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

flight_data=spark.read.csv("hdfs://namenode:9000/csv/2021-01.csv", schema=flightSchema)\
        .withColumn("FL_DATE",to_date(to_timestamp("FL_DATE", "M/d/yyyy h:mm:ss a")))

flight_data = flight_data.fillna({'DEP_DELAY_NEW':0.0})

flight_group = flight_data.groupBy('OP_UNIQUE_CARRIER', 'ORIGIN_AIRPORT_ID', 'DEST_AIRPORT_ID').agg(sum('DEP_DELAY_NEW').alias('delay'))

flight_group = flight_group.orderBy(desc('delay'))

flight_group.select('*').show(10)

#flight_data.select(flight_data.columns[:32]).show(1000)
#flight_data.printSchema()

# Delta table creation
#flight_data.write.format("delta").mode("overwrite").saveAsTable("sample_delta_table")
#DeltaTable.isDeltaTable(spark, "sample_delta_table") # True