# DAT500-Project-23V

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

## Commands Worth Remembering

``scp -r \<folder> \<user@ip>:~``

``start-dfs.sh``

``start-yarn.sh``

``hdfs dfsadmin -report``

``hadoop fs -put \<folder>/<*.txt> </dir>``

``"python3 <py_file>.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -r hadoop hdfs:///<dir>/<file> --output-dir hdfs:///<dir>/<file> --no-output``

``hadoop fs -text /\<folder>/\<file> | less``