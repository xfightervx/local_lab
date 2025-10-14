from pyspark.sql import SparkSession,DataFrame

def readFromDb(session : SparkSession, conf : dict) -> DataFrame:
    """ Read the currect untreated day data from the database to be transformed
    Args :
        - session (SparkSession) : the intial spark session with the db driver to use to read from database
        - cong (dict) : the dictionary that contain the connection parametres
    """
    url = f"jdbc:postgresql://{conf["POSTGRES_HOST"]}:{conf["POSTGRES_PORT"]}/{conf["POSTGRES_DB"]}"
    connection_properties = {
        "user":conf["POSTGRES_USER"],
        "password":conf["POSTGRES_PASSWORD"],
        "driver":"org.postgresql.Driver"
    }
    condition = "event_time BETWEEN ( SELECT MIN(event_time) FROM stg.rides_stream WHERE treated = 'No' ) AND ( ( SELECT MIN(event_time) FROM stg.rides_stream WHERE treated = 'No' ) + INTERVAL '1 day' )"
    query = f"( SELECT * FROM stg.rides_stream WHERE {condition}) AS Todays_Data"
    return session.read.jdbc(
        url=url,
        table=query,
        properties=connection_properties
    )


def writeToDb(df : DataFrame, conf : dict):
    """Write cleaned DataFrame to the database
    Args:
        - df (DataFrame) : The cleaned dataframe we want to write to the database
        - conf (dict) : the dictionary that contain the connection parametres
    """
    url = f"jdbc:postgresql://{conf["POSTGRES_HOST"]}:{conf["POSTGRES_PORT"]}/{conf["POSTGRES_DB"]}"
    connection_properties = {
        "user":conf["POSTGRES_USER"],
        "password":conf["POSTGRES_PASSWORD"],
        "driver":"org.postgresql.Driver"
    }
    df.write.jdbc(
        url=url,
        properties=connection_properties,
        table=conf["WAREHOUSE_DB"]
    )
