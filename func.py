import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import conf.py

def format_data(x):
        return (x["node_id"], json.dumps(x))

def send_data(rdd):
    if rdd is not None:
        data = rdd.collect()
        data = map(lambda item: json.loads(item),  data)
        formatted_rdd = sc.parallelize(data)

        formatted_rdd= formatted_rdd.map(lambda x: format_data(x))            
        # formatted_rdd= formatted_rdd.map(lambda x: (None, json.dumps(x)))

        formatted_rdd.saveAsNewAPIHadoopFile(
                path='-', 
                outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                keyClass="org.apache.hadoop.io.NullWritable",
                valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
                conf=conf.es_write_conf)