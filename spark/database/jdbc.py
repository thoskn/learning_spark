from pyspark.sql import DataFrame


def load(df: DataFrame, url: str, table: str, driver: str, mode: str = "overwrite"):
    df.write.format("jdbc").mode(mode).option("url", url).option(
        "dbtable", table
    ).option("driver", driver).save()
