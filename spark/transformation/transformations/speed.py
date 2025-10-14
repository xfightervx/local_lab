from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from .base import BaseTranformation

class SpeedTranfromation(BaseTranformation):
    def apply(self, df: DataFrame) -> DataFrame:
        res = df.withColumn("duration",(F.unix_timestamp("dropoff_time") - F.unix_timestamp("event_time")) / 60)
        res = res.withColumn(
            "avgSpeed",
            F.when(
                F.col("duration")>0,
                F.round((F.col("distance_km")/F.col("duration")*60),2)
            ).otherwise(None)
        )
        return res