import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from util import create_spark_session, read_csv


data_path = "sample-data/yellow_tripdata_sample_2019-01.csv"
data_asset_name = 'your_data_asset_name'
suite_name = "taxi_suite.demo"
checkpoint_name = 'placeholder_checkpoint'
batch_id = 'your_batch_id'


def run_validation(df):

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

    print("Result of validation: ", result.success)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = create_spark_session()
    df = read_csv(spark, data_path)
    run_validation(df)
    spark.stop()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
