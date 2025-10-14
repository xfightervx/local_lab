from pyspark.sql import DataFrame

class BaseTranformation:
    """
    Base Transformation to be inherited from while defining the transformations
    """
    def apply(self, df: DataFrame)-> DataFrame:
        raise NotImplementedError("Transformation must be implemented")