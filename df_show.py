import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
data = [("mike", 1), ("bond", 700)]
columns = ["name", "code"]

print("creating DF from data: ", data)

df = spark.createDataFrame(data).toDF(*columns)

# print("DataFrame is created. Showing it now.")
logger.info("DataFrame is created. Showing it now in logging.")

df.show()

print("collecting data now", df.collect())
