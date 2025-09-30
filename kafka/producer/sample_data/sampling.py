from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType,LongType, TimestampType, StructField, IntegerType, DoubleType

_SAMPLING_LENGTH = 1000

def main():
    spark = SparkSession.builder.appName("Sampling_data").getOrCreate()

    # Schema is optional if parquet already has schema, but here’s the corrected version:
    schema = StructType([
        StructField("VendorId", IntegerType(), False),
        StructField("tpep_pickup_datetime", TimestampType(), False),
        StructField("tpep_dropoff_datetime", TimestampType(), False),
        StructField("passenger_count", LongType(), False),
        StructField("trip_distance", DoubleType(), False),
        StructField("RatecodeId", LongType(), False),
        StructField("store_and_fwd_flag", StringType(), False),
        StructField("PULocationID", LongType(), False),
        StructField("DOLocationID", LongType(), False),
        StructField("fare_amount", DoubleType(), False),
        StructField("extra", DoubleType(), False),
        StructField("mta_tax", DoubleType(), False),
        StructField("tip_amount", DoubleType(), False),
        StructField("tolls_amount", DoubleType(), False),
        StructField("improvement_surcharge", DoubleType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("congestion_surcharge", DoubleType(), False),
        StructField("airport_fee", DoubleType(), False),
        StructField("cbd_congestion_fee", DoubleType(), False),
    ])

    df = spark.read.schema(schema).parquet(
        "kafka/producer/sample_data/yellow_tripdata_2025-01.parquet"
    )

    total_rows = df.count()
    ratio = min(1.0, _SAMPLING_LENGTH / total_rows)
    df_sampled = df.sample(withReplacement=False, fraction=ratio)
    df_sampled.write.mode("overwrite").format("parquet").save(
        "kafka/producer/sample_data/yellow_tripdata_2025-01-sample.parquet"
    )

if __name__ == "__main__":
    main()
