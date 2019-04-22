package com.jm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vector

import org.apache.spark.ml.feature.{IDF, HashingTF, Tokenizer}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.functions.{col, udf}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.feature.{HashingTF => MllibHashingTF}
import scala.util.Try
/**
 * Created by root on 2017/8/3.
 */
class TFIDFModel  {
//  Logger.getRootLogger.setLevel(Level.WARN)
//  val conf = new SparkConf().setAppName("RmdTrain_400_2000")
//  //                  .setMaster("local[4]")
//  val sc = new SparkContext(conf)
//  val sqlContext = new SQLContext(sc)
//  import sqlContext.implicits._
//
//  def calTFIDF(topN:Int): DataFrame = {
//
//    sqlContext.sql("use database")
//
//    // 因为要提取theme的关键词, 所以需要先做聚合,将相同主题文章放到一起
//    // select concat_ws(" ".collect_list(content) group by theme) from table_tf_idf 应该也可以
//    val dataGroupByTheme = sqlContext.sql(s"select theme,content from table_tf_idf").rdd
//      .map(row => (row.getString(0), row.getString(1).replaceAll("\\p{Punct}", " ")))// 去掉英文标点
//      .reduceByKey((x, y) => x + y) //聚合
//      .toDF("theme", "content") //转回DF
//
//    // 用于做分词, 结果可见前文
//    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
//    val wordsData = tokenizer.transform(dataGroupByTheme)
//
//    // 获取单词->Hasing的映射(单词 -> 哈希值)
//    //此处HashingTF属于mllib, 默认numFeatures为1<<20, 但是ml下的hashingTF却是1<<18, 要统一才能确保hash结果一致
//    val mllibHashingTF = new MllibHashingTF(1 << 18)
//    val mapWords = wordsData.select("words").rdd.map(row => row.getAs[ArrayBuffer[String]](0))
//      .flatMap(x => x).map(w => (mllibHashingTF.indexOf(w), w)).collect.toMap
//
//    // 计算出TF值
//    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
//    val featurizedData = hashingTF.transform(wordsData)
//
//    // 计算IDF值, 实际计算出来的形式为稀疏矩阵 [标签总数,[标签1,标签2,...],[标签1的值,标签2的值,...]]
//    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//    val idfModel = idf.fit(featurizedData)
//    val rescaledData = idfModel.transform(featurizedData)
//
//    // 将得到的数据按照tf-idf值从大到小排序,提取topN,并且将hashing id 转为单词
//    val takeTopN = udf { (v: Vector) =>
//      (v.toSparse.indices zip v.toSparse.values)
//        .sortBy(-_._2) //负值就是从大到小
//        .take(topN)
//        .map(x => mapWords.getOrElse(x._1, "null") + ":" + f"${x._2}%.3f".toString) // 冒号分隔单词和值,值取小数后三位
//        .mkString(";") } // 词语和值的对以;隔开(别用逗号,会与hive表格式的TERMINATED BY ','冲突)
//
//    rescaledData.select(col("theme"), takeTopN(col("features")).as("features"))
//
//    //    rescaledData.select("features", "theme").take(3).foreach(println)
//  }
//
//
//  // 将原始文件写入数据库
//  def data2DB() = {
//    case class Article(theme:String, content:String)
//    // 每个hdfs路径主题下都有很多文章
//    val doc1 = sc.wholeTextFiles("/sdl/data/20news-bydate-train/comp.windows.x")
//    val doc2 = sc.wholeTextFiles("/sdl/data/20news-bydate-train/comp.graphics")
//    val doc3 = sc.wholeTextFiles("/sdl/data/20news-bydate-train/misc.forsale")
//    val doc = doc1.union(doc2).union(doc3)
//
//    // 创建df, 并且把文章的换行和回车去掉
//    val df =  sqlContext.createDataFrame(doc.map(x => Article(x._1.split("/").init.last,x._2.replaceAll("\n|\r",""))))
//    df.registerTempTable("df")
//
//    // df入库的方式与 saveOutPut 函数相同,省略
//  }
//
//
//  // 将dataframe格式数据保存到数据库
//  def saveOutPut(output:String) = {
//
////    calTFIDF().registerTempTable("tfidf_result_table")
//
//    sqlContext.sql("use database")
//    sqlContext.sql( s"""drop table if exists $output""")
//    sqlContext.sql(
//      s"""create table IF NOT EXISTS $output (
//      theme  String,
//      tfidf_result   String
//      )
//      ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""")
//    val sql = s"insert overwrite table $output select theme,features from tfidf_result_table"
//
//    try {
//      sqlContext.sql(sql)
//    }
//    catch {
//      case e: Exception => log.error("load data to table error")
//        throw e
//    }
//  }
}
