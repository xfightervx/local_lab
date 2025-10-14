"""
In this file we apply the transformation we defined in the tranforamtions module to another just define the class and add it to the 
tranformations array below to be applied
"""
from pyspark.sql import SparkSession
from transformations import SpeedTranfromation,TimeFeaturesTransforamtion,FateMetricTranfromation
from utils_helper import readFromDb,writeToDb
from dotenv import load_dotenv
import os

def get_conf()-> dict:
    load_dotenv(".env")
    return {
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "localhost"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB","analytics"),
        "WAREHOUSE_DB": os.getenv("WAREHOUSE_DB", "warehouse"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }

spark = (
    SparkSession.builder
    .appName("Batch tranformations")
    .master("local[*]")
    .config("spark.jars.packages","org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)
conf = get_conf()
df = readFromDb(spark, conf)

transformations = [FateMetricTranfromation(),SpeedTranfromation()]


for t in transformations:
    df = t.apply(df)

writeToDb(df , conf)

spark.stop()