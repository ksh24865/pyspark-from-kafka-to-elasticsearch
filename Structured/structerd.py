#-*- coding:utf-8 -*-
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.sql.types import StructType, StringType, DecimalType, ArrayType



# json_schema = StructType()\
#     .add("sensor_id", StringType())\
#     .add("node_id", StringType())\
#     .add("values", ArrayType(StringType()))\
#     .add("timestamp", StringType())
json_schema = StructType()\
    .add("sensor_id", StringType())\
    .add("node_id", StringType())\
    .add("values", StringType())\
    .add("timestamp", StringType())
    

if __name__ == "__main__":
    sc = SparkContext()
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder.appName("thisistest2").getOrCreate()

    messages = spark\
        .readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "10.5.110.41:9092") \
        .option("subscribe", "sensor-data") \
        .load() \
        .select(from_json(col("value").cast("string"), json_schema).alias("parsed_data")) \
        .select(
        "parsed_data.sensor_id",
        "parsed_data.node_id",
        "parsed_data.values",
        "parsed_data.timestamp"
    )

    # print
    # df = messages \
    # .writeStream \
    # .format("console") \
    # .start()

    # ES로 보내기
    df = messages \
    .writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "tmp/") \
    .option("es.resource", "structured/_doc") \
    .option("es.nodes", "localhost") \
    .start()

    df.awaitTermination()
    # print_data.awaitTermination()