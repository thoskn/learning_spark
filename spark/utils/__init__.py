from pyspark.sql import SparkSession


def session_builder(postgrs_driver_jar_path: str) -> SparkSession.builder:
    return (
        SparkSession.builder.master("local[*]")
        .appName("spark_stuff")
        .config("spark.driver.extraClassPath", postgrs_driver_jar_path)
    )
