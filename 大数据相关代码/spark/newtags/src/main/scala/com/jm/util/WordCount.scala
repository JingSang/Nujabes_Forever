package com.jm.util

import com.jm.util.ml.IDFUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object WordCount {
  def main(args: Array[String]): Unit = {
      //for (i <- 0 until 9) println(i)
      val conf=new SparkConf().setAppName("IDFTagsTrain_20170912")
      .setMaster("local[4]")
      val sc=new SparkContext(conf)
      val c=sc.accumulator(0)
      //val a=sc.textFile("file:///C:/Users/Administrator/Documents/Tencent Files/2545261616/FileRecv/MobileFile/data/*").flatMap(line => line.split(" ")).map(x=>(x,1)).reduceByKey(_+_).map(_._1).repartition(1).saveAsTextFile("file:///F:/Tags/Mydata")
      val a:RDD[(String,String)]=sc.textFile("file:///F:/Tags/article.txt",3).map(x=>{
        c.add(1)
        (c,Seq("你好","我爱","Nujabes","Sing02","re:plus","Ne-Yo","Niha","Ina","西原建一郎","Nieve","GEMINI","二宫爱"))
      }).map(x=>{
        val aid=x._1.toString
        val arr:Seq[(String,String)]=x._2.map(x=>{
          (aid,x)
        })
        //aid tag
        arr
      }).flatMap(x=>x)
//    a.foreach(i => {
//      println("partitionId：" + TaskContext.get.partitionId)
//    })
    val b:Map[String,Long]=sc.textFile("file:///F:/Tags/article.txt",3).map(x=>{
      ("1",Seq("你好","我爱","Nujabes","Sing02","re:plus","Ne-Yo","Niha","Ina","西原建一郎","Nieve","GEMINI","二宫爱"))
    }).map(x=>{
      val aid=x._1.toString
      val arr:Seq[String]=x._2.map(x=>{
        x
      })
      //println(arr)
      arr
    }).flatMap(x=>x).map(x=>(x,1)).reduceByKey(_+_).map(_._1).zipWithIndex().collect().toMap
//    a.foreach(println)
//    b.foreach(println)
    IDFUtils.createFile("/user/data/test/tmp1","/user/data/test/tmp2",a,b)
  }
}
