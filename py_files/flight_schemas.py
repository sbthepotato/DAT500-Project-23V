from pyspark.sql.types import StructType

flightSchema = StructType() \
	.add('YEAR', 'integer')\
	.add('QUARTER', 'integer')\
	.add('MONTH', 'integer')\
	.add('DAY_OF_MONTH', 'integer')\
	.add('DAY_OF_WEEK', 'integer')\
	.add('FL_DATE', 'string')\
	.add('OP_UNIQUE_CARRIER', 'string')\
	.add('TAIL_NUM', 'string')\
	.add('OP_CARRIER_FL_NUM', 'integer')\
	.add('ORIGIN_AIRPORT_ID', 'integer')\
	.add('ORIGIN_AIRPORT_SEQ_ID', 'integer')\
	.add('ORIGIN_CITY_MARKET_ID', 'integer')\
	.add('ORIGIN', 'string')\
	.add('ORIGIN_CITY_NAME', 'string')\
	.add('ORIGIN_STATE_NM', 'string')\
	.add('DEST_AIRPORT_ID', 'integer')\
	.add('DEST_AIRPORT_SEQ_ID', 'integer')\
	.add('DEST_CITY_MARKET_ID', 'integer')\
	.add('DEST', 'string')\
	.add('DEST_CITY_NAME', 'string')\
	.add('DEST_STATE_NM', 'string')\
	.add('DEP_DELAY', 'float')\
	.add('DEP_DELAY_NEW', 'float')\
	.add('DEP_DEL15', 'float')\
	.add('ARR_DELAY', 'float')\
	.add('ARR_DELAY_NEW', 'float')\
	.add('ARR_DEL15', 'float')\
	.add('CANCELLED', 'float')\
	.add('CANCELLATION_CODE', 'float')\
	.add('DIVERTED', 'float')\
	.add('AIR_TIME', 'float')\
	.add('DISTANCE', 'float')\
	.add('DISTANCE_GROUP', 'float')\
	.add('CARRIER_DELAY', 'float')\
	.add('WEATHER_DELAY', 'float')\
	.add('NAS_DELAY', 'float')\
	.add('SECURITY_DELAY', 'float')\
	.add('LATE_AIRCRAFT_DELAY', 'float')

numIdSchema = StructType()\
	.add('id', 'integer')\
  .add('val', 'string')

StringIdSchema = StructType()\
	.add('id', 'string')\
  .add('val', 'string')
