# 版本
  java 1.7
  
  cdh 5.8.11
  
  scala 2.10
  
  spark 1.6
    
# 背景

  目前nifi不支持直接从源直接写入kudu，所以先到kafka，在实时写入kudu
  

# 可配置选项
```
Usage: DirectKafkaWordCount <brokers> <topics>
  <brokers> is a list of one or more Kafka brokers
  <topics> is a list of one or more kafka topics to consume from
  <masterList> is a list of kudu
  <kuduTableName> is a name of kudu
  <appName>  is a name of spark processing
  <dataProcessingMode> the function of dataProcessing logical processing mode
         default,common,newcommon,stds,tcdns
  <groupid> is the name of kafka groupname
  <checkpoint_path> is a address of checkpoint  hdfs:///tmp/checkpoint_$name
```

spark-submit --jars spark-streaming-kafka-0-8_2.10-2.1.0.jar,kudu-spark_2.10-1.3.0.jar --master yarn --deploy-mode client spark1kafka9.jar cmname1:9092,cmdata1:9092,cmdata2:9092,cmserver:9092 domain cmname1 impala::default.tsgz_tcdns DirectKafka_tcdns tcdns tcdns hdfs:///tmp/checkpoint_tcdns

spark-submit --jars spark-streaming-kafka-0-8_2.10-2.1.0.jar,kudu-spark_2.10-1.3.0.jar --master yarn --deploy-mode cluster spark1kafka9.jar cmname1:9092,cmdata1:9092,cmdata2:9092,cmserver:9092 syslognifi cmname1 impala::default.tsgz_syslog_new DirectKa_syslog_new newcommon syslog_new hdfs:///tmp/checkpoint_syslog_new

spark-submit --executor-memory 1G --executor-cores 2 --driver-cores 1 --driver-memory 1G --queue public --jars spark-streaming-kafka-0-8_2.10-2.1.0.jar,kudu-spark_2.10-1.3.0.jar --master yarn --deploy-mode cluster spark1kafka9.jar cmname1:9092,cmdata1:9092,cmdata2:9092,cmserver:9092 domaincmname1 impala::default.tsgz_tcdns DirectKafka_tcdns tcdns tcdns hdfs:///tmp/checkpoint_tcdns

# jar包




