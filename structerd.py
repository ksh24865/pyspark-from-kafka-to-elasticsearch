from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DecimalType, ArrayType
from pyspark.sql.functions import *


json_schema = StructType()\
    .add("sensor_id", DecimalType())\
    .add("node_id", DecimalType())\
    .add("values", ArrayType(DecimalType()))\
    .add("timestamp", StringType())
    

if __name__ == "__main__":
    sc = SparkContext()
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder.appName("thisistest").getOrCreate()

    messages = spark\
        .readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "10.5.110.41:9092") \
        .option("subscribe", "sensor-data") \
        .load() \
        .select(from_json(col("value").cast("string"), json_schema).alias("parsed_logs")) \
        .select(
        "parsed_logs.sensor_id",
        "parsed_logs.node_id",
        "parsed_logs.values",
        "parsed_logs.timestamp"
    )
    query = messages \
    .writeStream \
    .format("console") \
    .start()

    query.awaitTermination()