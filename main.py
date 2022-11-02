# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


def create_spark():
    spark = ge.core.util.get_or_create_spark_application()
    return spark

def read_csv(path):
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    return df

def run_validation(df):
    # Set up a basic spark session
    # spark = ge.core.util.get_or_create_spark_application()
    df.show()

    data_asset_name = 'your_data_asset_name'
    suite_name = "taxi_suite.demo"
    checkpoint_name = 'placeholder_checkpoint'
    batch_id = 'your_batch_id'

    batch_request = RuntimeBatchRequest(datasource_name="my_spark_dataframe",
                                        data_connector_name="default_runtime_data_connector_name",
                                        data_asset_name=data_asset_name,
                                        runtime_parameters={"batch_data": df},
                                        batch_identifiers={"default_identifier_name": batch_id}, )

    context = ge.get_context()

    result = context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        validations=[{"batch_request": batch_request, 'expectation_suite_name': suite_name}, ],
    )


def run_in_mem_validation():
    df = read_csv(data_path)
    run_validation(df)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print_hi('PyCharm')
    spark = create_spark()
    data_path = "sample-data/yellow_tripdata_sample_2019-01.csv"
    run_in_mem_validation()
    spark.stop()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
