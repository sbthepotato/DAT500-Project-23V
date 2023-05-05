# DAT500-Project-23V

## overview of the directory

- pre-presentation/ - contains the LaTeX files for the pre project presentation
- presentation/ - contains the LaTeX files for the project presentation
- py_files/ - contains all of the full python files used in the project
- report/ - contains the LaTeX files for the project report
- spark-setup/ - contains the spark configuration file and how we set up spark.
- commands.txt - contains useful and often used commands in the application
- htmlplots.7z - contains the html output files when plotting the dataset

## Datasets

The datasets have been sourced from the [US Bureau of Transportation statistics](https://www.transtats.bts.gov/Tables.asp?QO_VQ=EFD&QO_anzr=Nv4yv0r%FDb0-gvzr%FDcr4s14zn0pr%FDQn6n&QO_fu146_anzr=b0-gvzr).

columns downloaded:

- Time Period:
  - Year
  - Quarter
  - Month
  - DayofMonth
  - DayofWeek
  - FlightDate
- Airline:
  - Reporting_Airline
  - Tail_Number
  - Flight_Number_Reporting_Airline
- Origin:
  - OriginAirportID
  - OriginAirportSeqID
  - OriginCityMarketID
  - Origin
  - OriginCityName
  - OriginStateName
- Destination:
  - DestAirportID
  - DestAirportSeqID
  - DestCityMarketID
  - Dest
  - DestCityName
  - DestStateName
- Departure Performance:
  - DepDelay
  - DepDelayMinutes
  - DepDel15
- Arrival Performance:
  - ArrDelay
  - ArrDelayMinutes
  - ArrDel15
- Cancellations and Diversions:
  - Cancelled
  - CancellationCode
  - Diverted
- Flight Summaries
  - AirTime
  - Distance
  - DistanceGroup
- Cause of Delay
  - CarrierDelay
  - WeatherDelay
  - NASDelay
  - SecurityDelay
  - LateAircraftDelay

## Pyspark documentation link

[Apache Spark - Pyspark](https://spark.apache.org/docs/3.2.1/api/python/getting_started/index.html)
