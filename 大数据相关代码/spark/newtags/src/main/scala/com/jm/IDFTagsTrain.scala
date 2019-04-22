package com.jm

import java.io.File
import java.util.Properties

import com.hankcs.hanlp.dictionary.CustomDictionary
import com.hankcs.hanlp.tokenizer.NLPTokenizer
import com.jm.util.KafkaSink
import com.jm.util.ml.IDFUtils
import nlp.segment.SegmentUtils
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF => MllibHashingTF}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, WrappedArray}


/**
 * Created by root on 2016/12/27.
 */
object IDFTagsTrain {
  val PARAMS1 = List("nr","nr1","nr2","nrj","nrf")
  val PARAMS2 = List("ntc","nt","ntcb","ntcf","nth","ntch","nto","nts","ntu")
  val PARAMS3 = List("ns","nsf")
  val PARAMS4 = List("nb","nba","nbc","nbp","nf","nh","nhd","nhm","nm","nmc")
  val PARAMS5 = List("ni","nic","nis","nit","nl","nn","nnd","nnt","n","nz","nx")
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("IDFTagsTrain_20170912")
    // .setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //每次计算之后结果data和model 日期 yyyyMMdd
    val daytime:String=args(1)
    //最终结果存放hbase 日期yyyy-MM-dd
    val daytimeOther:String=args(2)


    val inputPath="hdfs://10.70.5.26:8020/user/chenmeng_test/article_tmp/*"
    val idfModelPath = "hdfs://10.70.5.26:8020/user/data/fenci/models-"+daytime+"-"+args(0)+"/vectorize"
    val segmentPath="hdfs://10.70.5.26:8020/user/data/ckooc-ml/segment.properties"
    val keywordtrainPath="hdfs://10.70.5.26:8020/user/data/fenci/data-"+daytime+"-"+args(0)+"/news/keywordtrain"
    val vectorizePath="hdfs://10.70.5.26:8020/user/data/fenci/data-"+daytime+"-"+args(0)+"/news/vectorize"
    val saveToHdfsPath1="/user/data/test/tmp1"
    val saveToHdfsPath2="/user/data/test/tmp2"
//      val inputPath="file:///F:/Tags/rcmd_article_content1"
//      //"file:///F:/Tags/segment.properties"
//      val segmentPath="hdfs://10.70.5.26:8020/user/data/ckooc-ml/segment.properties"
//      val keywordtrainPath="file:///F:/Tags/keywordtrain"
//      val vectorizePath="file:///F:/Tags/vectorize"
//      val saveToHdfsPath1="/user/data/test/tmp1"
//      val saveToHdfsPath2="/user/data/test/tmp2"
    //分词提取关键词  hdfs://10.70.5.26:8020/user/hive/warehouse/hive.db/rcmd_article_content/part-m-00002
    val input =sc.textFile(inputPath).map(_.split("\001"))
      .filter(x=>{
        x.length==9 && x(8).length>0 && !"".equals(x(8)) && x(8)!=null
      })
    val rdd: RDD[(Long, String, String)]=input.map(x=>(x(0).toLong,x(7),x(8)))
    val preUtils: SegmentUtils = SegmentUtils(segmentPath)
    val result: RDD[(Long, Seq[String])] =preUtils.runSummary(rdd)
    result.map(x=>x._1+"\t"+x._2.mkString(" ")).saveAsTextFile(keywordtrainPath)

    //tfidf  model 计算
    val splitedRDD: RDD[(Long, Seq[String])] =sc.textFile(keywordtrainPath).map(_.split("\t")).filter{x=>x.length>1 && x(1).nonEmpty }.map(x=>(x(0).toLong,getSeq(x(1)))).map(line => (line._1, line._2.get))

    val tokenDF: DataFrame = splitedRDD.map(x=>(x._1,x._2.mkString(" "))).toDF("id", "tokens")
    val tokenizer: Tokenizer = new Tokenizer().setInputCol("tokens").setOutputCol("words")
    val wordsData: DataFrame = tokenizer.transform(tokenDF)

    // 获取单词->Hasing的映射(单词 -> 哈希值)
    //此处HashingTF属于mllib, 默认numFeatures为1<<20, 但是ml下的hashingTF却是1<<18, 要统一才能确保hash结果一致
    val mllibHashingTF = new MllibHashingTF(1 << 18)
    val mapWords: Map[Int, String] = wordsData.select("words").rdd.map(row => row.getAs[WrappedArray[String]](0))
      .flatMap(x => x).map(w => (mllibHashingTF.indexOf(w), w)).collect().toMap
                                                    // aid  cid gid time
    val tagMap:Map[Int,(Int,Int,Int)]=input.map(x=>(x(0).toInt,(x(1).toInt,x(3).toInt,x(5).toInt))).collect().toMap
    //val mapTitle:Map[Long, Array[String]]=

    //println(tagMap.toArray.toBuffer)

    val hashingTF: HashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures")
    val featurizedData: DataFrame =hashingTF.transform(wordsData)
    var startTime = System.nanoTime()
    val lpTime = (System.nanoTime() - startTime) / 1e9
    startTime = System.nanoTime()
    //  val features: RDD[Vector] =tokensLP.map(_.features)
    val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel: IDFModel = idf.fit(featurizedData)
    val idfTime = (System.nanoTime() - startTime) / 1e9
    println(s"转化TFIDF完成！\n\t 耗时: $idfTime sec\n")
    //idfModel.save(idfModelPath+File.separator+"IDF")
    val lastResult: DataFrame =idfModel.transform(featurizedData)
    val lastresult: RDD[(Long, Seq[(String, Double)])] =IDFUtils.saveResult(lastResult,mapWords,daytimeOther,"train")
    //hdfs://10.70.5.26:8020/user/data/JMRecommend/models-"+daytime+"-"+args(0)+"/vectorize
    lastresult.toJavaRDD.saveAsTextFile(vectorizePath)
//    val indexWithTag:Array[((String,String),Long)]=lastresult.map(x=>{
//      val aid = x._1.toString
//      val array: Seq[(String, Double)] = x._2
//      val arr:Seq[(String,String)]=array.map(x=>{
//        (aid,x._1)
//      })
//      arr
//    }).flatMap(x=>x).zipWithIndex().collect()
    val aidWithTag:RDD[(String,String)]=lastresult.map(x=>{
      val aid = x._1.toString
      val array: Seq[(String, Double)] = x._2
      val arr:Seq[(String,String)]=array.map(x=>{
        //aid ,tag
        (aid,x._1)
      })
      arr
    }).flatMap(x=>x)
    val tagsIdMap:Map[String,Int]=aidWithTag.map(x=>(x._2,1)).reduceByKey(_+_).map(_._1).zipWithIndex().map(x=>(x._1,x._2.toInt)).collect().toMap
    IDFUtils.createFile(saveToHdfsPath1,saveToHdfsPath2,aidWithTag,tagsIdMap,tagMap)
    //IDFUtils.saveToMysql(indexWithTag)
    //IDFUtils.writeToKafka(lastresult,kafkaProducer,sc)
    //IDFUtils.createFile(saveToHdfsPath,indexWithTag,tagMap)
    //IDFUtils.loadData2Hive(saveToHdfsPath)
  }

  def getSeq(str:String):  Option[Seq[String]] ={
    var arrayBuffer = ArrayBuffer[String]()
    if (str != null && str != "") {
      arrayBuffer+=str
      Some(arrayBuffer)
    } else {
      None
    }
  }


}
