import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import countDistinct
from redis import Redis

from database import redis
from database.jdbc import build_postgres_url, extract
from utils import session_builder


def join(
    people_df: DataFrame, items_df: DataFrame, transactions_df: DataFrame
) -> DataFrame:
    df = transactions_df.join(
        people_df, on=transactions_df["PERSON_ID"] == people_df["ID"], how="left"
    )
    df = df.join(items_df, on=transactions_df["ITEM_ID"] == items_df["ID"], how="left")
    # TODO get all transactions for each person on the same partition? Is there a benefit?
    return df


def unique_users(redis_client: Redis, df: DataFrame):
    # Could have used the df only containing people, but that would be boring
    print(df.agg(countDistinct("ID")).collect())

    # TODO below
    # TODO get a good way of interacting for debugging
    # TODO above

    redis.load(redis_client, "num_users", df.agg(countDistinct("PERSON_ID")).collect())


def aggregate(
    spark_session: SparkSession, postgres_url: str, driver: str, redis_client: Redis
):
    people_df = extract(spark_session, postgres_url, "PEOPLE", driver)
    items_df = extract(spark_session, postgres_url, "ITEMS", driver)
    transactions_df = extract(spark_session, postgres_url, "TRANSACTIONS", driver)
    df = join(people_df, items_df, transactions_df).cache()

    # Number of unique users with transactions
    # Number of transactions
    # Number of unique items with transactions
    # Top N items
    # Top N people
    # Age distribution
    # Age distribution for top items
    # Distribution of number of items per person
    # Distribution of number of transactions per person
    unique_users(redis_client, df)


if __name__ == "__main__":
    POSTGRES_HOST = os.environ["POSTGRES_HOST"]
    DATABASE_NAME = os.environ["DATABASE_NAME"]
    POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
    POSTGRES_USER = os.environ["POSTGRES_USER"]
    POSTGRS_DRIVER_JAR_PATH = os.environ["POSTGRS_DRIVER_JAR_PATH"]
    POSTGRES_DRIVER_CLASS = os.environ["POSTGRES_DRIVER_CLASS"]
    REDIS_HOST = os.environ["REDIS_HOST"]

    POSTGRES_URL = build_postgres_url(
        POSTGRES_HOST, DATABASE_NAME, POSTGRES_USER, POSTGRES_PASSWORD
    )

    spark = session_builder(POSTGRS_DRIVER_JAR_PATH).getOrCreate()
    redis_c = Redis(host=REDIS_HOST)
    t0 = time.time()
    aggregate(spark, POSTGRES_URL, POSTGRES_DRIVER_CLASS, redis_c)
    load_time = time.time() - t0
    print(load_time)
