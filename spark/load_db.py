import glob
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import split

from database import csv, jdbc
from utils import session_builder


def get_df_from_csv_directory(spark_session: SparkSession, directory: str) -> DataFrame:
    files = glob.glob(os.path.join(directory, "*.csv"))
    for idx, f in enumerate(files):
        if idx == 0:
            df = csv.extract(spark_session, f)
        else:
            df = df.union(csv.extract(spark_session, f))
    return df


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

    data_directory = sys.argv[1]

    spark = session_builder(POSTGRS_DRIVER_JAR_PATH).getOrCreate()

    people_df = get_df_from_csv_directory(spark, os.path.join(data_directory, "person"))
    people_df.show()

    people_df = transform(people_df)
    people_df.show()

    jdbc.load(people_df, POSTGRES_URL, "people", POSTGRES_DRIVER_CLASS)
