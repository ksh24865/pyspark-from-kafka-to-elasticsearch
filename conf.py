es_ip = "localhost"
es_port = "9200"
es_index = "test_index"
es_mapping_id = "sensor_id"
kf_ip = "10.5.110.41"
kf_port = "9092"
kf_topic = "sensor-data"

es_write_conf = {
    "es.nodes": es_ip,
    "es.port": es_port,
    "es.resource": es_index+"/_doc",
    "es.input.json": "yes",        
    "es.mapping.id": es_mapping_id
    # "es.index.auto.create": "true",
    # sparkConf.set("es.nodes.wan.only", "true")
    # sparkConf.set("es.net.http.auth.pass", "")
    # "mapred.reduce.tasks.speculative.execution": "false",
    # "mapred.map.tasks.speculative.execution": "false"
}
