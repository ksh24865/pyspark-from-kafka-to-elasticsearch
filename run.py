import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import conf
import func

if __name__ == "__main__":

    sc = SparkContext(appName="pyspark-from-kafka-to-elasticsearch")
    
    ssc = StreamingContext(sc, 20)    
    kvs = KafkaUtils.createDirectStream(ssc, [conf.kf_topic], {"metadata.broker.list": conf.kf_ip+":"+conf.kf_port})

    dStream = kvs.map(lambda x: x[1])
    print("~!!!!!!!!!!!!!!!!!",type(dStream),"!~!~!~@~!@#~!@#$!@")
    dStream.pprint()
    dStream.foreachRDD(lambda rdd: func.send_data(sc,rdd))

    ssc.start()
    ssc.awaitTermination()

