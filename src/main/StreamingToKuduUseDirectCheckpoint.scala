package main

import kafka.serializer.StringDecoder
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingToKuduUseDirectCheckpoint {

  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <masterList> is a list of kudu
                            |  <kuduTableName> is a name of kudu
                            |  <appName>  is a name of spark processing
                            |  <dataProcessingMode> the function of dataProcessing logical processing mode
                            |         default,common,newcommon,stds,tcdns
                            |  <groupid> is the name of kafka groupname
                            |  <checkpoint_path> is a address of checkpoint  hdfs:///tmp/checkpoint_
        """.stripMargin)
      System.exit(1)
    }

    //1.获取输入参数与定义全局变量,主要两部分，一个brokers地址，一个topics列表，至于其他优化参数后续再加
    //TODO: 注意：后面需要改为properties的方式来获取参数
    val Array(brokers,topic,masterList,kuduTableName,appName,dataProcessingMode,groupid,checkpoint_path) = args
/*    val appName = "DirectKafkaWordCount"
    val masterList = "cmname1:7051"
    val kuduTableName = "stds"*/
    val sctime = 10

    //2.配置spark环境以及kudu环境
    val sparkConf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(sparkConf, Seconds(sctime))
    //ssc.checkpoint("hdfs:///tmp/checkpoint_stds")
    ssc.checkpoint(checkpoint_path)

    val sqlContext = new SQLContext(ssc.sparkContext)

    val kuduContext = new KuduContext(masterList)

    //3.配置创建kafka输入流的所需要的参数，注意这里可以加入一些优化参数
    val topics = Set("json")
    //0.9
/*    val kafkaParamsq = Map[String, String](
      "metadata.broker.list" -> "cmname1:9092,cmdata1:9092,cmdata2:9092,cmserver:9092",
      "group.id" -> "tttttttttttttttt",
      "auto.offset.reset" -> "smallest"
    )*/
    val kafkaParamsq = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupid,
      "auto.offset.reset" -> "smallest"
    )

    //4.创建kafka输入流0.10
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParamsq,topic.split(",").toSet)

    //5.对每一个window操作执行foreach，写入数据处理逻辑
    // TODO：这里要写一个数据逻辑处理函数，不同的数据对应不同的函数,函数返回的就是DF，然后直接操作DF写入目的地

    stream.foreachRDD { rdd =>

      //获取所有的value，不知道这里key为什么都是null？
      //val messageRDD = rdd.map(record => (record.key(), record.value())).values

      val messageRDD = rdd.map(_._2)
      messageRDD.foreach(println(_))

      //将value映射成多个字段,也就是将rdd转换成dataframe
/*      //方法1
      val schemaString = "sys_id,sys_hostname,sys_time,sys_message"
      val schema = StructType(schemaString.split(",").map(fieldName=>StructField(fieldName,StringType,true)))
      val kuduRDD = messageDF.map(_.split(",")).map(p=>Row(p(0),p(1),p(2),p(3)))
      val kuduDF = spark.createDataFrame(kuduRDD,schema)
      kuduDF.show(false)

      //方法2
      case class syslog(sys_id: String, sys_hostname: String, sys_time: String, sys_message: String)
      import spark.implicits._
      val kuduDF1 = messageDF.map(_.split(",")).map(p=>syslog(p(0),p(1),p(2),p(3))).toDF()
      kuduDF.show(false)*/

      //syslog
      //val kuduDF = KafkaKudu.utils.dataProcessing.syslogPorcess(dataProcessingMode,messageRDD,spark)
      //stds
      //val dataProcessingMode = "stds"
      val kuduDF = utils.dataProcessing.Porcess(dataProcessingMode,messageRDD,sqlContext)

      //messageDF.createOrReplaceTempView("syslogTableTemp")
      if(kuduDF == null){
        System.err.println("dataProcessingMode选择错误： " + dataProcessingMode + "\n" +
                            "dataProcessingMode可以为default,common,newcomer")
        System.exit(1)
      }
      kuduContext.upsertRows(kuduDF,kuduTableName)

    }

    //6.Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
