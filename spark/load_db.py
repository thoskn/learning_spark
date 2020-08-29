import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, split

from database import csv, jdbc
from utils import session_builder


def transform(df: DataFrame) -> DataFrame:
    # Assumes only ever two names
    return df.withColumn("first_name", split("name", " ").getItem(0)).withColumn(
        "last_name", split("name", " ").getItem(1)
    )


if __name__ == "__main__":
    DATABASE_NAME = os.environ["DATABASE_NAME"]
    POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
    POSTGRES_USER = os.environ["POSTGRES_USER"]
    POSTGRS_DRIVER_JAR_PATH = os.environ["POSTGRS_DRIVER_JAR_PATH"]
    POSTGRES_DRIVER_CLASS = os.environ["POSTGRES_DRIVER_CLASS"]

    POSTGRES_URL = f"jdbc:postgresql://localhost:5432/{DATABASE_NAME}?user={POSTGRES_USER}&password={POSTGRES_PASSWORD}"

    spark = session_builder(POSTGRS_DRIVER_JAR_PATH).getOrCreate()

    people_df = csv.extract(spark, "data/people.csv")
    people_df.show()

    people_df = transform(people_df)
    people_df.show()

    jdbc.load(people_df, POSTGRES_URL, "people", POSTGRES_DRIVER_CLASS)
