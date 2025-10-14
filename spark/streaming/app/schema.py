from pyspark.sql.types import  StringType,StructType,StructField,IntegerType,TimestampType,LongType,DoubleType

schema = StructType([
    StructField("ride_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("ingest_time", TimestampType(), False),
    StructField("dropoff_time", TimestampType(), False),
    StructField("pickup_zone", StringType(), True),
    StructField("dropoff_zone", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_usd", DoubleType(), True),
    StructField("payment_type", StringType(), True),
])