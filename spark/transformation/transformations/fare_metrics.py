from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from .base import BaseTranformation

class FateMetricTranfromation(BaseTranformation):
    def apply(self, df: DataFrame) -> DataFrame:
        """Calculate simple fare metrics as fare per km and fare per minute
        Args:
            - df (DataFrame) : the dataframe to extract informations from
        Returns:
            - res (DataFrame) : the enriched dataframe
        """
        res = df.withColumn("duration",(F.unix_timestamp("dropoff_time") - F.unix_timestamp("event_time")) / 60)
        return res.withColumns({
            "fate_per_km": F.when(
                F.col("distance_km")> 0 , F.round(F.col("fare_usd")/F.col("distance_km"),3)
            ).otherwise(None),
            "fare_per_min":F.when(
                F.col("duration")>0 , F.round(F.col("fare_usd")/F.col("duration"), 3)
            ).otherwise(None)
        })