# pyspark-from-kafka-to-elasticsearch



## Version
```
Python: 3.8.0
Spark: spark-2.4.7-bin-hadoop2.7
Pyspark: 3.1.1
ElasticSearch: 7.6.1
Kafka: 2.5.0
Zookeeper: 3.6.1 
```

## Run 

* Spark Streaming(Dstreams)
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --jars jar/elasticsearch-spark-20_2.11-7.6.1.jar Dstream/run.py
```

* Structured Streaming
```
park-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --jars jar/elasticsearch-spark-20_2.11-7.6.1.jar --conf spark.cores.max=2 Structured/structerd.py
```

## Output
* Consumed data from Kafka

  <img width="540" alt="image (1)" src="https://user-images.githubusercontent.com/55729930/110294095-c2367100-8032-11eb-8408-da7c88816b29.png">

* Data stored in ES
 
  <img width="500" alt="image (2)" src="https://user-images.githubusercontent.com/55729930/110294099-c3679e00-8032-11eb-97de-1de56a89cd18.png">
