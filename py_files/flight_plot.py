#!/usr/bin/env python3

from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from delta import *
import plotly.graph_objs as plt
from plotly.offline import plot

def normalize(df, feature_name):
    result = df.copy()
    max_value = df[feature_name].max()
    min_value = df[feature_name].min()
    result[feature_name] = (
        df[feature_name] - min_value) / (max_value - min_value)
    return result[feature_name]

builder = SparkSession.builder.appName('flight_plot')
spark = configure_spark_with_delta_pip(builder).getOrCreate()

flight_data = spark.read.format('delta').load('hdfs://namenode:9000/spark-warehouse/flight_data_table')

flight_data = flight_data.filter((flight_data.op_unique_carrier == 'AA') & (flight_data.origin_airport_id == 12892) & (flight_data.dest_airport_id == 12478))

flight_data = flight_data.orderBy('year', 'month')

flight_data = flight_data.withColumn('year_month', concat('year', lit('-'), 'month'))

#flight_data.select('*').show()

flight_data = flight_data.toPandas()

avgdelay = normalize(flight_data, "avg_arr_delay")
flightcount = normalize(flight_data, "flight_count")

data = [
	plt.Scatter(x=flight_data.year_month
						, y=avgdelay
						, name='average flight delay over time'
            , text=flight_data.avg_arr_delay),
	plt.Scatter(x=flight_data.year_month
						, y=flightcount
						, name='nr of flights'
            , text=flight_data.flight_count),
]

fig = plt.Figure(data, layout_title_text='delay over time with nr of flights normalized')
plot(fig, filename='plot.html')