from pyspark.sql import DataFrame

from pyspark.sql import SparkSession


def create_spark_session():
    spark = SparkSession.builder.appName("My GE App").getOrCreate()
    return spark


def read_csv(spark, path) -> DataFrame:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    return df
