import sys
import json

from pyspark import SparkContext

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 20)

    kvs = KafkaUtils.createDirectStream(ssc, ["sensor-data"], {"metadata.broker.list": "10.5.110.3:9092"})

    es_write_conf = {
        "es.nodes": "10.5.110.1",
        "es.port": "9200",
        "es.resource": "my_index2/_doc",
        "es.input.json": "yes"
        # "es.index.auto.create": "true",
        # sparkConf.set("es.nodes.wan.only", "true")
        # sparkConf.set("es.net.http.auth.pass", "")
        # "mapred.reduce.tasks.speculative.execution": "false",
        # "mapred.map.tasks.speculative.execution": "false"
        # "es.mapping.id": "sensor_id"
    }

    def format_data(x):
        return (x["sensor_id"], json.dumps(x))

    def send_data(rdd):
        if rdd is not None:
            data = rdd.collect()
            data = map(lambda item: json.loads(item),  data)

            formatted_rdd = sc.parallelize(data)

            # formatted_rdd= formatted_rdd.map(lambda x: format_data(x))
            # print("~!@#!@$!@$%!@#%!@#%!@#%^@#%@#$%@#%@#%@#%@#",formatted_rdd,data)
            formatted_rdd= formatted_rdd.map(lambda x: (None, json.dumps(x)))
            formatted_rdd.saveAsHadoopFile(
                path='-',
                outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                keyClass="org.apache.hadoop.io.NullWritable",
                valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                conf=es_write_conf
                )
            # formatted_rdd.saveAsNewAPIHadoopFile(
            #         path='-', 
            #         outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            #         keyClass="org.apache.hadoop.io.NullWritable",
            #         valueClass="org.apache.hadoop.io.Text",
            #         #valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            #         conf=es_write_conf)
    dStream = kvs.map(lambda x: x[1])
    # dStream.pprint()
    dStream.foreachRDD(lambda rdd: send_data(rdd))

    ssc.start()
    ssc.awaitTermination()

