"""
Spark Structured Streaming Job
------------------------------
Consumes JSON messages from Kafka topic `rides_raw`, parses them into structured
columns using schema.py + transforms.py, and writes results to Postgres
(analytics.rides_stream).
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
import pandas as pd
from schema import schema
from transforms import parse_kafka, apply_transforms
from sqlalchemy import create_engine


def get_config():
    load_dotenv(".env")
    return {
        "BOOTSTRAP_SERVERS": os.getenv("BOOTSTRAP_SERVERS", "localhost:9092"),
        "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", "rides_raw"),
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "localhost"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB", "analytics"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "BATCH_INTERVAL": os.getenv("BATCH_INTERVAL", "5 seconds"),
    }

def write_to_postgres(batch_df: DataFrame, batch_id: int, cfg: dict):
    """Utile function to write the batch into the sql table using the connection

    Args:
        batch_df (DataFrame): Spark Dataframe to write into the DB
        batch_id (int): a unique identifier for the batch
        cfg (dict): DB connection configuration

    Returns:
        int: execution status
    """
    if batch_df.isEmpty():
        return 

    pdf = batch_df.toPandas()

    url = (
        f"postgresql+psycopg2://{cfg['POSTGRES_USER']}:{cfg['POSTGRES_PASSWORD']}"
        f"@{cfg['POSTGRES_HOST']}:{cfg['POSTGRES_PORT']}/{cfg['POSTGRES_DB']}"
    )
    try :
        engine = create_engine(url)
    except :
        print("couldn't create connection with the database")
        return
    pdf.to_sql(
        "rides_stream",
        con=engine,
        schema="stg",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    print(f"batch {batch_id} has been written to the database successfully")
    return

def main():
    cfg = get_config()
    spark = (
        SparkSession.builder
        .appName("rides_streaming")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1"
        )
        .getOrCreate()
    )

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg["BOOTSTRAP_SERVERS"])
        .option("subscribe", cfg["KAFKA_TOPIC"])
        .load()
    )
    parsed_df = parse_kafka(raw_df, schema)
    cleaned_df = apply_transforms(parsed_df)
    query = (
        cleaned_df.writeStream
        .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, cfg))
        .outputMode("append")
        .trigger(processingTime=cfg["BATCH_INTERVAL"])
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
