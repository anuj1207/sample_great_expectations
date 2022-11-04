from pyspark.sql import DataFrame

import great_expectations as ge


def create_spark_session():
    spark = ge.core.util.get_or_create_spark_application()
    return spark


def read_csv(spark, path) -> DataFrame:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    return df
