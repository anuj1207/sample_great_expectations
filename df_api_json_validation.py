import great_expectations as ge
from util import create_spark_session, read_csv

data_path = "sample-data/yellow_tripdata_sample_2019-01.csv"
checks_json = "great_expectations/metadata/expectations/taxi_suite/release-1.json"


def run_validation(df):
    # Set up a basic spark session
    # spark = ge.core.util.get_or_create_spark_application()

    ge_df = ge.dataset.SparkDFDataset(df)
    print("Result of validation: ", ge_df.validate(expectation_suite=checks_json).success)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = create_spark_session()
    df = read_csv(spark, data_path)
    run_validation(df)
    spark.stop()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
