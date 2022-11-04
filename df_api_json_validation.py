# This is a sample Python script.
from pyspark.sql import DataFrame

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import great_expectations as ge


data_path = "sample-data/yellow_tripdata_sample_2019-01.csv"
checks_json = "great_expectations/metadata/expectations/taxi_suite/demo.json"

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


def create_spark_session():
    spark = ge.core.util.get_or_create_spark_application()
    return spark


def read_csv(path) -> DataFrame:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    return df


def run_validation(df):
    # Set up a basic spark session
    # spark = ge.core.util.get_or_create_spark_application()

    ge_df = ge.dataset.SparkDFDataset(df)
    print(ge_df.validate(expectation_suite=checks_json).success)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print_hi('PyCharm')
    spark = create_spark_session()
    df = read_csv(data_path)
    run_validation(df)
    spark.stop()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
