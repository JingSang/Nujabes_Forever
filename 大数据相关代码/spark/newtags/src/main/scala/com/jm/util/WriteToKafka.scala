package com.jm.util

import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WriteToKafka {
  def main(args: Array[String]): Unit = {
      Logger.getRootLogger.setLevel(Level.WARN)
      val conf=new SparkConf().setMaster("local").setAppName("app")
      val sc:SparkContext=new SparkContext(conf)
      val rdd:RDD[String]=sc.parallelize(Array("1","2","3","4"))
      // 广播KafkaSink
      val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
        val kafkaProducerConfig = {
          val p = new Properties()
          p.setProperty("bootstrap.servers", "10.70.6.26:9092")
          p.setProperty("key.serializer", classOf[StringSerializer].getName)
          p.setProperty("value.serializer", classOf[StringSerializer].getName)
          p
        }
      sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    rdd.foreach(record=>{
      kafkaProducer.value.send("test",record)
    })
  }
}
