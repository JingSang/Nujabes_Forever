package com.jm.util.ml

import java.security.MessageDigest
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import com.hankcs.hanlp.dictionary.CustomDictionary
import com.hankcs.hanlp.tokenizer.NLPTokenizer
import com.jm.util.{HiveDBUtil, KafkaSink}
import com.jm.util.hbase.HbaseTool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.Set
import org.apache.spark.{Logging, SparkContext, TaskContext}

/**
 * Created by root on 2017/8/3.
 */
object IDFUtils extends Logging {
  val PARAMS1 = List("nr","nr1","nr2","nrj","nrf")
  val PARAMS2 = List("ntc","nt","ntcb","ntcf","nth","ntch","nto","nts","ntu")
  val PARAMS3 = List("ns","nsf")
  val PARAMS4 = List("nb","nba","nbc","nbp","nf","nh","nhd","nhm","nm","nmc")
  val PARAMS5 = List("ni","nic","nis","nit","nl","nn","nnd","nnt","n","nz","nx")
  def saveResult(result: DataFrame, mapWords: Map[Int, String] ,daytimeother:String,actionType:String): RDD[(Long, Seq[(String, Double)])] ={

    // 将得到的数据按照tf-idf值从大到小排序,提取topN,并且将hashing id 转为单词
    val lastresult: RDD[(Long, Seq[(String, Double)])] =result.select("id","features").map{case Row(id:Long,features:Vector)=>
      val indices: Array[Int] =features.toSparse.indices
      val values: Array[Double] =features.toSparse.values
      //val array:Array[String]=mapTitle.getOrElse(id,null)
      val words: Array[String] =indices.map(index=>mapWords.getOrElse(index,"null")).filter(x=>{
        val regex = "^[\u4e00-\u9fa5]+$"
        x.matches(regex)
      })
      val seq: Seq[(String, Double)] =words.zip(values).toSeq
      var res=seq.map(x=>{
        var pos=0.0
        var loc=0.0
        var w=1.0
        var len=0.0
        val word:String= NLPTokenizer.segment(x._1).get(0).toString
        val arr=word.split("/")(1)
        if(arr.length>0&&arr.charAt(0)=='n'&&arr.length<=8){
          if(PARAMS1.contains(arr)){
            pos=5.0
          }else if (PARAMS2.contains(arr)){
            pos=2.0
          }else if(PARAMS3.contains(arr)){
            pos=1.5
          }else if(PARAMS4.contains(arr)){
            pos=1.0
          }else if(PARAMS5.contains(arr)){
            pos=0.5
          }
        }
        if(x._1.length==2) len=0.2
        if(x._1.length==3) len=0.3
        if(x._1.length==4) len=0.4
        if(x._1.length==5) len=0.5
//        for (i <- 0 until array.length){
//          if (x._1.equals(array(i))) {
//            loc = 2.5
//          }
//        }
        (x._1,x._2*(pos+loc+len+w))
      }).sortBy(-_._2).take(20)
      res=res.map(x=>{
        var pos=1.0
        if(CustomDictionary.contains(x._1)){
          pos=3.0
        }
        (x._1,x._2*pos)
      }).sortBy(-_._2).take(10)
      (id,res)
    }
    lastresult



//   val thisresult: RDD[String] = lastresult.map(x=>{
//      val keywords: Seq[(String, Double)] =x._2
//      val iter=keywords.iterator
//      var str=new StringBuffer()
//      while(iter.hasNext){
//        val key: (String, Double) =iter.next()
//        val st: String =key._1+"\t"+x._1+"_"+key._2+","
//        str.append(st)
//      }
//      str.toString().split(",").map(x=>{
//        val array: Array[String] =x.split("\t")
//        val key=array(0)
//        val aid=array(1)
//        (key,aid)
//      })
//    }).flatMap(x=>x).aggregateByKey(zeroValue)(seqOp, combOp).map(x=>{
//      val mllibHashingTF = new MllibHashingTF(1 << 18)
//      val rowkey=mllibHashingTF.indexOf(x._1)+"_"+x._1
//      var aids=x._2.toArray.mkString(",")
//
//
//      rowkey+"\t"+aids
//    })
//    thisresult.saveAsTextFile("hdfs://1.node.ta.jiemian-inc.com:8020/user/data/JMRecommend/keywordswithaid")

//    val thatresult: RDD[String] =lastresult.map( x =>{
//      val aid = x._1.toString
//      val array: Seq[(String, Double)] = x._2
//      val iter = array.iterator
//      val str: StringBuffer = new StringBuffer()
//      while (iter.hasNext) {
//        val keyword = iter.next()
//        str.append(keyword._1 + "_" + keyword._2 + ",")
//      }
//      var st: String = ""
//      if (str.indexOf(",") > -1) {
//        st = str.substring(0, str.length() - 1)
//      } else {
//        st = "null"
//      }
//      val rowkey=getArcid(aid)+"_"+aid
//      rowkey+"\t"+st
//    })
//    thatresult.saveAsTextFile("hdfs://1.node.ta.jiemian-inc.com:8020/user/data/JMRecommend/aidwithkeywords")



  }
  def getArcid(aid:String):String={
    val ed=new StringBuffer(aid).reverse().toString()
    //    println(ed)
    ed
  }
  def saveArticleTagsKeywordsAids(lastresult: RDD[(Long, Seq[(String, Double)])],actionType:String): Unit ={
    val zeroValue = Set[String]()
    val seqOp = (set: Set[String], value: String) => {
      set += value
      set
    }
    val combOp  = (set1: Set[String], set2: Set[String]) => {
      set1 ++= set2
      set1
    }
    lastresult.map(x=>{
      val keywords: Seq[(String, Double)] =x._2
      val iter=keywords.iterator
      var str=new StringBuffer()
      while(iter.hasNext){
        val key: (String, Double) =iter.next()
        val st: String =key._1+"\t"+x._1+"_"+key._2+","
        str.append(st)
      }
      str.toString().split(",").map(x=>{
        val array: Array[String] =x.split("\t")
        val key=array(0)
        val aid=array(1)
        (key,aid)
      })
    }).flatMap(x=>x).aggregateByKey(zeroValue)(seqOp, combOp).foreachPartition(x=>x.foreach(x=>{
//      val mllibHashingTF = new MllibHashingTF(1 << 18)
//      val rowkey=mllibHashingTF.indexOf(x._1)+"_"+x._1
	  val rowkey=md5s(x._1)+"_"+x._1
      var aids=x._2.toArray.mkString(",")
      if("predict".equals(actionType)){
        val arry: Array[(String, String)] =HbaseTool.getValue("article_tags_keywords_aids",rowkey, "keywordswithaids",Array("keywordswithaid"))
        val value: String =arry.map(x=>x._1+"_"+x._2).mkString(",")
		if(value.nonEmpty){
          aids=value+","+aids
        }
      }
      HbaseTool.putValue("article_tags_keywords_aids",rowkey, "keywordswithaids",Array(("keywordswithaid",aids)))

    }))
  }
  def saveArticleTagsAidsKeywords(lastresult: RDD[(Long, Seq[(String, Double)])]): Unit ={
    lastresult.foreachPartition(x=>x.foreach( x =>{
      val aid = x._1.toString
      val array: Seq[(String, Double)] = x._2
      val iter = array.iterator
      val str: StringBuffer = new StringBuffer()
      while (iter.hasNext) {
        val keyword = iter.next()
        str.append(keyword._1 + "_" + keyword._2 + ",")
      }
      var st: String = ""
      if (str.indexOf(",") > -1) {
        st = str.substring(0, str.length() - 1)
      } else {
        st = "null"
      }
      val rowkey=getArcid(aid)+"_"+aid
//      println(rowkey+"======="+st)
      HbaseTool.putValue("article_tags_aids_keywords",rowkey, "aidwithkeywords",Array(("aidwithkeyword",st)))

    }))
  }


  def md5s(plainText:String):String={
    val md: MessageDigest =MessageDigest.getInstance("MD5")
    md.update(plainText.getBytes("UTF-8"))
    val b: Array[Byte] =md.digest()
    var i=0
    val buf=new StringBuffer("")
    for (offset <- 0 until b.length) {
         i=b(offset)
         if(i<0){
           i+=256
         }
         if(i<16){
           buf.append("0")
         }
      buf.append(Integer.toHexString(i))
    }
      buf.toString
  }
  def praseToJsonAndSave(lastresult: RDD[(Long, Seq[(String, Double)])]):Unit={
    lastresult.foreachPartition(x=>{
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = "insert into tags(id, tags) values (?, ?)"
      try {
        conn = DriverManager.getConnection("jdbc:mysql://10.70.7.26:3306/huangtaoTest?useUnicode=true&characterEncoding=utf8","root", "123456")
        x.foreach(x=>{
          val aid = x._1.toString
          val array: Seq[(String, Double)] = x._2
          val iter = array.iterator
          var str: StringBuffer = new StringBuffer()
          str.append("{\"reslut\":[")
          while (iter.hasNext) {
            val keyword = iter.next()
            str.append("{\"name\":\""+keyword._1+"\",\"value\":\""+keyword._2+"\"},")
          }
          if (str.indexOf(",") > -1) {
            str = new StringBuffer(str.substring(0, str.length() - 1))
          } else {
            str=str
          }
          str.append("]}")
          ps = conn.prepareStatement(sql)
          ps.setString(1, aid)
          ps.setString(2,str.toString)
          ps.executeUpdate()
        })
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }
  def writeToKafka(lastresult: RDD[(Long, Seq[(String, Double)])],kafkaProducer: Broadcast[KafkaSink[String, String]],sc:SparkContext):Unit={
    lastresult.foreachPartition(x=>{

      x.foreach(record=>{
        val aid = record._1.toString
        val array: Seq[(String, Double)] = record._2
        val iter = array.iterator
        while(iter.hasNext){
          val tag=iter.next()._1
          kafkaProducer.value.send("test",aid+"\t"+tag)
        }
      })
    })
  }
  def saveToMysql(indexWithTag:Array[((String,String),Long)]):Unit={
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into tags(tagid,aid,tag) values (?, ?,?)"
    var connectStr="jdbc:mysql://10.70.7.26:3306/huangtaoTest?useUnicode=true&characterEncoding=utf8"
    connectStr += "&useServerPrepStmts=false&rewriteBatchedStatements=true"
    val step=indexWithTag.length/10000
    var tagid=0
    var aid=0
    var tag=""
    try {
      conn = DriverManager.getConnection(connectStr,"root", "123456")
      conn.setAutoCommit(false) // 设置手动提交
      ps = conn.prepareStatement(sql)
      var count=0
      for(i<- 0 until step){
        for(j<- 0 until 10000){
          println(count)
          tagid=indexWithTag(count)._2.toInt
          aid=indexWithTag(count)._1._1.toInt
          tag=indexWithTag(count)._1._2
          ps.setInt(1,tagid)
          ps.setInt(2,aid)
          ps.setString(3,tag)
          ps.addBatch()
          count=count+1
        }
        ps.executeBatch()
        conn.commit
        println(count)
      }
     while(count<indexWithTag.length){
       tagid=indexWithTag(count)._2.toInt
       aid=indexWithTag(count)._1._1.toInt
       tag=indexWithTag(count)._1._2
       ps.setInt(1,tagid)
       ps.setInt(2,aid)
       ps.setString(3,tag)
       ps.addBatch()
       count=count+1
      }
      println(count)
      ps.executeBatch()
      conn.commit
    } catch {
      case e: Exception => e.printStackTrace()
        println(tagid,aid,tag)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }
  //aid ,tag,tagid
  def createFile(dst:String,indexWithTag:Array[((String,String),Long)],timeMap:Map[String,String]):Unit={
    val conf = new Configuration
    //System.setProperty("HADOOP_USER_NAME","chenmeng")
    val fs = FileSystem.get(conf)
    val dstPath = new Path(dst)//目标路径
    val outputStream = fs.create(dstPath)//打开一个输出流
    val sb = new StringBuffer
    for(i<-indexWithTag){
      val time=timeMap.getOrElse(i._1._1,"null")
      sb.append(i._2+"\001"+i._1._1+"\001"+i._1._2+"\001"+time+"\n")
    }
    sb.deleteCharAt(sb.length - 1)
    //println(sb.toString)
    val contents = sb.toString.getBytes
    outputStream.write(contents)
    outputStream.close()
    fs.close()
    println("文件创建成功！")
  }
  def createFile(dst:String,dst1:String,aidWithTag:RDD[(String,String)],tagWithid:Map[String,Long]):Unit= {
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    //目标路径
    //打开一个输出流
    val outputStream = fs.create(new Path(dst1))
    val sb2 = new StringBuffer
    for(i<-tagWithid){
      sb2.append(i._2+"\001"+i._1+"\n")
    }
    sb2.deleteCharAt(sb2.length - 1)
    val contents2:Array[Byte] = sb2.toString.getBytes
    outputStream.write(contents2)
    outputStream.close()

    if(fs.exists(new Path(dst))){
      fs.delete(new Path(dst),true)
    }
    aidWithTag.map(x=>{
      val aid=x._1
      val tag=x._2
      val tagid=tagWithid.getOrElse(tag,0)
      aid+"\001"+tagid
    }).saveAsTextFile(dst)
    println("文件创建成功！")
    fs.close()
  }
  def createFile(dst:String,dst1:String,aidWithTag:RDD[(String,String)],tagWithid:Map[String,Int],tagMap:Map[Int,(Int,Int,Int)]):Unit= {
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    //目标路径
    //打开一个输出流
    val outputStream = fs.create(new Path(dst1))
    val sb2 = new StringBuffer
    for(i<-tagWithid){
      sb2.append(i._2+"\001"+i._1+"\n")
    }
    sb2.deleteCharAt(sb2.length - 1)
    val contents2:Array[Byte] = sb2.toString.getBytes
    outputStream.write(contents2)
    outputStream.close()

    if(fs.exists(new Path(dst))){
      fs.delete(new Path(dst),true)
    }
    aidWithTag.map(x=>{
      val aid=x._1
      val tag=x._2
      val tagid=tagWithid.getOrElse(tag,0)
      val otherDeatil:(Int,Int,Int)=tagMap.getOrElse(aid.toInt,(0,0,0))
      aid+"\001"+tagid+"\001"+otherDeatil._1+"\001"+otherDeatil._2+"\001"+otherDeatil._3
    }).saveAsTextFile(dst)
    println("文件创建成功！")
    fs.close()
  }
  def loadData2Hive(dst: String,table:String):Unit={
    var con: Connection = null
    var ps: PreparedStatement = null
    val sql = " load data inpath '" + dst + "' into table hive."+table
    try{
      con=HiveDBUtil.getConn
      ps = con.prepareStatement(sql)
      ps.execute()
      println("插入成功")
    }catch {
      case e:Exception=>e.printStackTrace()
    }
  }
  def downloadDataExternalHive(dst: String,table:String):Unit={

  }
  def main(args: Array[String]): Unit = {
    //loadData2Hive("/user/data/test/tmp1/part-*","tags_aid_detail")
  }
}
