package com.jm.util.hbase

/**
 * Created by root on 2016/9/30.
 */

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging

import scala.collection.mutable

object HbaseTool extends Logging with Serializable{

  val table = new mutable.HashMap[String,HTable]()
  var conf: Configuration = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "10.70.5.26,10.70.6.26,10.70.7.26")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.client.keyvalue.maxsize","524288000");//最大500m
//  conf.addResource(new Path("/etc/hbase/conf.cloudera.hbase/hbase-site.xml"))
//  conf.addResource(new Path("/etc/hbase/conf.cloudera.hbase/core-site.xml"))
  def setConf(c:Configuration)={
    conf = c
  }

  def getTable(tableName:String):HTable={

    table.getOrElse(tableName,{
      println("----new connection ----")
      val tbl = new HTable(conf, tableName)
      table(tableName)= tbl
      tbl
    })
  }

  def getValue(tableName:String,rowKey:String,family:String,qualifiers:Array[String]):Array[(String,String)]={
    var result:AnyRef = null
    val table_t =getTable(tableName)
    val row1 =  new Get(Bytes.toBytes(rowKey))
    val HBaseRow = table_t.get(row1)
    if(HBaseRow != null && !HBaseRow.isEmpty){
      result = qualifiers.map(c=>{
        (tableName+"."+c, Bytes.toString(HBaseRow.getValue(Bytes.toBytes(family), Bytes.toBytes(c))))
      })
    }
    else{
      result=qualifiers.map(c=>{
        (tableName+"."+c,"null")  })
    }
    result.asInstanceOf[Array[(String,String)]]
  }

  def putValue(tableName:String,rowKey:String, family:String,qualifierValue:Array[(String,String)]) {
    val table =getTable(tableName)
    val new_row  = new Put(Bytes.toBytes(rowKey))
    qualifierValue.map(x=>{
      var column: String = x._1
      val value: String = x._2
      new_row.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
      val strdate=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
      println("时间"+strdate+"写入Hbase"+ rowKey)
    })
      table.put(new_row)
  }
  def deleteColumn(tableName:String,rowKey:String, family:String,column:Array[(String)]): Unit ={
    val table =getTable(tableName)
    val new_row  = new Delete(Bytes.toBytes(rowKey))
    column.map(x=>{
      if(!(x.isEmpty)){
//        new_row.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
        new_row.deleteColumns(Bytes.toBytes(family),Bytes.toBytes(x))
      }

    })
    table.delete(new_row)
  }
  def main(args: Array[String]) {
    val c:Configuration=new Configuration()
    c.set("hbase.zookeeper.quorum", "10.70.5.26")
    c.set("hbase.zookeeper.property.clientPort", "2181")
//    putValue(c,"article_recommended_result","123131232131"+"_"+"1", "recommended",Array(("2016-10-08","12321314343242_article")))
  }
//  val family = "F"
}
