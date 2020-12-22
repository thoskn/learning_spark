from typing import Optional

from pyspark.sql import DataFrame, SparkSession


def build_postgres_url(host: str, database_name: str, user: str, password: str):
    return (
        f"jdbc:postgresql://{host}:5432/{database_name}?user={user}&password={password}"
    )


def load(
    df: DataFrame,
    url: str,
    table: str,
    driver: str,
    mode: str = "overwrite",
    max_partitions: Optional[int] = None,
):
    """Write to output jdbc compatible database"""
    if max_partitions:
        # TODO check whether this is expensive
        partitions = df.rdd.getNumPartitions()
        if partitions > max_partitions:
            df = df.repartition(max_partitions)
    df.write.jdbc(url=url, table=table, mode=mode, properties={"driver": driver})


def extract(
    spark_session: SparkSession, url: str, table: str, driver: str
) -> DataFrame:
    return spark_session.read.jdbc(url=url, table=table, properties={"driver": driver})
