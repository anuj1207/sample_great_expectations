# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

from util import create_spark_session, read_csv

data_path = "sample-data/yellow_tripdata_sample_2019-01.csv"


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = create_spark_session()
    df = read_csv(spark, data_path)
    df.show()
    spark.stop()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
