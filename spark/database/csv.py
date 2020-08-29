from pyspark.sql import DataFrame, SparkSession


def extract(
    spark_session: SparkSession, file_path: str, header: str = "true"
) -> DataFrame:
    if header not in ["true", "false"]:
        raise ValueError("header must be either `true` or `false`")
    return spark_session.read.format("csv").option("header", header).load(file_path)
