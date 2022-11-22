from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def create_sample_df(df) -> DataFrame:
    data = [("jackal", 1), ("mikeson", 21)]
    columns = ["name", "id"]
    df = df.withColumn('age', F.lit("100"))
    return df