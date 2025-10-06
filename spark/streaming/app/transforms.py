from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F

def parse_kafka(df: DataFrame, schema: StructType) -> DataFrame:
    """Util function to parse the json message based on a given schema
    Args:
        df (DataFrame): A stream based dataframe from the kafka stream read
        schema (StructType): schema definition
    
    Returns:
        df (DataFrame): parsed json of the stream dataframe
    """
    df = df.selectExpr("CAST(value as String) as json_str")
    parsed = df.select(F.from_json(F.col("json_str"),schema).alias("data"))
    parsed = parsed.select("data.*")
    return parsed


def apply_transforms(df: DataFrame)-> DataFrame:
    """Util apply basic transformation to the DataFrame
    
    Args:
        df (DataFrame): flawed Spark DataFrame

    return:
        parsed (DataFrame): rectified DataFrame
    """
    parsed = (
        df.withColumn("distance_km", F.round(F.col("distance_km"), 2))
          .withColumn("fare_usd", F.abs(F.col("fare_usd")))
          .withColumn(
              "payment_type",
              F.when(F.lower(F.col("payment_type")) == "unknown", F.lit(None))
               .otherwise(F.col("payment_type"))
          )
    )
    return parsed