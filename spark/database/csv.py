import glob
import os

from pyspark.sql import DataFrame, SparkSession


def extract(
    spark_session: SparkSession, file_path: str, header: str = "true"
) -> DataFrame:
    if header not in ["true", "false"]:
        raise ValueError("header must be either `true` or `false`")
    return spark_session.read.format("csv").option("header", header).load(file_path)


def get_df_from_csv_directory(spark_session: SparkSession, directory: str) -> DataFrame:
    files = glob.glob(os.path.join(directory, "*.csv"))
    for idx, f in enumerate(files):
        if idx == 0:
            df = extract(spark_session, f)
        else:
            df = df.union(extract(spark_session, f))
    return df
