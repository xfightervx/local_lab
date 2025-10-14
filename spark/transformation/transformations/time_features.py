from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from .base import BaseTranformation


class TimeFeaturesTransforamtion(BaseTranformation):
    def apply(self, df: DataFrame) -> DataFrame:
        return super().apply(df)