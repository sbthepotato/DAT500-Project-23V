#!/usr/bin/env python3

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import to_timestamp, to_date

spark = SparkSession.builder.getOrCreate()

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
    .add("DEP_DELAY", "double")\
    .add("DEP_DELAY_NEW", "double")\
    .add("DEP_DEL15", "double")\
    .add("ARR_DELAY", "double")\
    .add("ARR_DELAY_NEW", "double")\
    .add("ARR_DEL15", "double")\
    .add("CANCELLED", "double")\
    .add("CANCELLATION_CODE", "string")\
    .add("DIVERTED", "double")\
    .add("AIR_TIME", "double")\
    .add("DISTANCE", "double")\
    .add("DISTANCE_GROUP", "integer")\
    .add("CARRIER_DELAY", "double")\
    .add("WEATHER_DELAY", "double")\
    .add("NAS_DELAY", "double")\
    .add("SECURITY_DELAY", "double")\
    .add("LATE_AIRCRAFT_DELAY", "double")

flight_data=spark.read.csv("hdfs://namenode:9000/2022-12.csv", schema=flightSchema)\
        .withColumn("FL_DATE",to_date(to_timestamp("FL_DATE", "M/d/yyyy h:mm:ss a")))

flight_data.select(flight_data.columns[:11]).show(15)

flight_data.printSchema()