import os
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import split

from database import csv, jdbc
from database.jdbc import build_postgres_url
from utils import session_builder


def transform_people(df: DataFrame) -> DataFrame:
    # Assumes only ever two names
    return (
        df.withColumn("FIRST_NAME", split("NAME", " ").getItem(0))
        .withColumn("LAST_NAME", split("NAME", " ").getItem(1))
        .withColumn("AGE", df["AGE"].cast("INT"))
    )


def transform_items(df: DataFrame) -> DataFrame:
    return df.withColumn("PRICE", df["PRICE"].cast("FLOAT"))


def transform_transactions(df: DataFrame) -> DataFrame:
    return df


def load_people(
    spark_session: SparkSession,
    data_dir: str,
    postgres_url: str,
    postgres_driver_class: str,
):
    people_df = csv.get_df_from_csv_directory(
        spark_session, os.path.join(data_dir, "person")
    )

    people_df = transform_people(people_df)

    jdbc.load(people_df, postgres_url, "people", postgres_driver_class)


def load_items(
    spark_session: SparkSession,
    data_dir: str,
    postgres_url: str,
    postgres_driver_class: str,
):
    items_df = csv.get_df_from_csv_directory(
        spark_session, os.path.join(data_dir, "item")
    )

    items_df = transform_items(items_df)

    jdbc.load(items_df, postgres_url, "items", postgres_driver_class)


def load_transactions(
    spark_session: SparkSession,
    data_dir: str,
    postgres_url: str,
    postgres_driver_class: str,
):
    transactions_df = csv.get_df_from_csv_directory(
        spark_session, os.path.join(data_dir, "transaction")
    )

    transactions_df = transform_transactions(transactions_df)

    jdbc.load(transactions_df, postgres_url, "transactions", postgres_driver_class)


if __name__ == "__main__":
    POSTGRES_HOST = os.environ["POSTGRES_HOST"]
    DATABASE_NAME = os.environ["DATABASE_NAME"]
    POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
    POSTGRES_USER = os.environ["POSTGRES_USER"]
    POSTGRS_DRIVER_JAR_PATH = os.environ["POSTGRS_DRIVER_JAR_PATH"]
    POSTGRES_DRIVER_CLASS = os.environ["POSTGRES_DRIVER_CLASS"]

    POSTGRES_URL = build_postgres_url(
        POSTGRES_HOST, DATABASE_NAME, POSTGRES_USER, POSTGRES_PASSWORD
    )

    data_directory = sys.argv[1]

    spark = session_builder(POSTGRS_DRIVER_JAR_PATH).getOrCreate()
    t0 = time.time()
    load_people(spark, data_directory, POSTGRES_URL, POSTGRES_DRIVER_CLASS)
    load_items(spark, data_directory, POSTGRES_URL, POSTGRES_DRIVER_CLASS)
    load_transactions(spark, data_directory, POSTGRES_URL, POSTGRES_DRIVER_CLASS)
    load_time = time.time() - t0
    print(f"Loaded in {load_time} seconds")
