# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import great_expectations as ge
from util import create_spark_session, read_csv

data_path = "sample-data/yellow_tripdata_sample_2019-01.csv"


def run_validation(df):
    # Set up a basic spark session
    # spark = ge.core.util.get_or_create_spark_application()

    gde = ge.dataset.SparkDFDataset(df)
    print("Result of validation:\n", gde.expect_column_min_to_be_between("passenger_count", 1, 10))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = create_spark_session()
    df = read_csv(spark, data_path)
    run_validation(df)
    spark.stop()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
