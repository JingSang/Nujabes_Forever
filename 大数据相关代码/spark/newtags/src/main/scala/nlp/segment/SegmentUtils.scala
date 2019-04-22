package nlp.segment

import java.io._
import java.util
import java.util.Properties

import com.hankcs.hanlp.HanLP
//import com.hankcs.hanlp.corpus.io.HadoopFileIoAdapter
import com.hankcs.hanlp.seg.CRF.CRFSegment
import com.hankcs.hanlp.seg.common
import com.hankcs.hanlp.tokenizer.NLPTokenizer
import com.huaban.analysis.jieba.JiebaSegmenter
import nlp.segment.chinese.ZHConverter
import org.ansj.domain.Term
import org.ansj.recognition.impl.FilterRecognition
import org.ansj.splitWord.analysis._
import org.ansj.util.MyStaticValue
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
  * 数据预处理类，包含以下操作：
  * <p>&nbsp &nbsp &nbsp &nbsp基本清洗（繁简转换、全半角转换、去除无意义词）、分词、分句、分段、去除停用词、去除低频词</p>
  * <p>Created by yhao on 2016/3/12.</p>
  */
class SegmentUtils(config: SegmentConfig) extends Logging with Serializable {

  private val enExpr = "[A-Za-z]+".r
  //英文字符正则
  private val numExpr = "\\d+(\\.\\d+)?(\\/\\d+)?".r
  //数值正则，可以匹配203,2.23,2/12
  private val baseExpr =
    """[^\w-\s+\u4e00-\u9fa5]""".r //匹配英文字母、数字、中文汉字之外的字符

  private val zhConverter = ZHConverter.getInstance(1)

  /**
    * 参数列表
    */
  val PARAMS = List("g","gb","gbc","gc","gg","gi","gm","gp","ntc","ntcb","ntcf","nth","ntch","nts","ntu","nx","nz"
    ,"nb","nba","nbp","nf","ng","nh","nhd","nhm","nm","nmc","nr","nr1","nr2","nrf","nrj","n","nt"
    ,"nsf")
  /**
    * 对sc的textFile方法的封装，可以按指定的最小块进行切分读取
    *
    * @param sc      SparkContext
    * @param inPath  输入路径
    * @param minSize 最小块大小
    * @return RDD[String]
    */
  def getText(sc: SparkContext, inPath: String, minSize: Int = 32): RDD[String] = {
    val hadoopConf = sc.hadoopConfiguration
    val fs = new Path(inPath).getFileSystem(hadoopConf)
    val len = fs.getContentSummary(new Path(inPath)).getLength / (1024 * 1024) //以MB为单位的数据大小
    val minPart = (len / minSize).toInt //按minSize的分块数

    sc.textFile(inPath, minPart)
  }


  /**
    * 全角转半角
    *
    * @param line 输入数据
    * @return 转换为半角的数据
    */
  def q2b(line: String): String = {
    zhConverter.convert(line)
  }


  /**
    * 繁体转简体，使用hanlp工具包方法实现
    *
    * @param line 输入数据
    * @return 转换为简体的数据
    */
  def f2j(line: String): String = {
    HanLP.convertToSimplifiedChinese(line)
  }


  /**
    * 简体转繁体，使用hanlp工具包方法实现
    *
    * @param line 输入数据
    * @return 转换为繁体的数据
    */
  def j2f(line: String): String = {
    HanLP.convertToTraditionalChinese(line)
  }


  /**
    * 针对单行记录
    * 基础清理，包括:繁转简体、全角转半角、去除不可见字符、数值替换、去英文字符
    *
    * @param line 输入数据
    * @return 经过基础清洗的数据
    */
  def baseClean(line: String): String = {
    var result = line.trim
    val numToChar = config.numToChar

    //繁简转换
    if (config.f2j) {
      result = f2j(result)
    }

    //全半角转换
    if (config.q2b) {
      result = q2b(result)
    }

    //去除不可见字符
//    result = baseExpr.replaceAllIn(result, "")
//    result = StringUtils.trimToEmpty(result)

    //替换数字
    if (config.delNum) {
      result = numExpr.replaceAllIn(result, numToChar)
    }

    //去除英文字符
    if (config.delEn) {
      result = enExpr.replaceAllIn(result, "")
    }

    result
  }


  /**
    * 使用ansj分词工具进行分词
    *
    * @param text          待分词文本
    * @param stopwordArray 停用词数组
    * @return 分词结果
    */
  def ansjSegment(text: String, stopwordArray: Array[String]): util.List[Term] = {
//  def ansjSegment(text: String, stopwordArray: RDD[String]): util.List[Term] = {
    val filter = new FilterRecognition()
    for (stopword <- stopwordArray) {
      filter.insertStopWord(stopword)
    }
//    stopwordArray.map(x=>{filter.insertStopWord()})

    val splitType = config.splitType

    MyStaticValue.ambiguityLibrary

    val result = splitType match {
      case "BaseAnalysis" => BaseAnalysis.parse(text).recognition(filter)
      case "ToAnalysis" => ToAnalysis.parse(text).recognition(filter)
      case "DicAnalysis" => DicAnalysis.parse(text).recognition(filter)
      case "IndexAnalysis" => IndexAnalysis.parse(text).recognition(filter)
      case "NlpAnalysis" => NlpAnalysis.parse(text).recognition(filter)
      case _ =>
        println("分词方式不对，请检查splitType（BaseAnalysis/ToAnalysis/DicAnalysis/IndexAnalysis/NlpAnalysis）")
        sys.exit(1)
    }

    result.getTerms
  }


  /**
    * 分词,每行返回一个Seq[String]的分词结果
    *
    * @param text          原文本
    * @param stopwordArray 停用词数组
    * @return 分词结果
    */
  def wordSegment(text: String, stopwordArray: Array[String]): Option[Seq[String]] = {
//  def wordSegment(text: String, stopwordArray: RDD[String]): Option[Seq[String]] = {
    var arrayBuffer = ArrayBuffer[String]()
    val splitTool = config.splitTool

    if (text != null && text != "") {
      val tmp = new util.ArrayList[Term]()

      splitTool match {
        case "ansj" =>
          val result: util.List[Term] = ansjSegment(text, stopwordArray)
          tmp.addAll(result)
        case _ =>
          println("分词工具错误，请检查splitTool（ansj/hanlp）")
          sys.exit(1)
      }

      val addNature = config.addNature
      for (i <- 0 until tmp.size()) {
        val term = tmp.get(i)
        if (addNature) {
          val item = term.getName
          val nature = term.getNatureStr
          arrayBuffer += item + "/" + nature
        } else {
          var item = term.getName.trim()
          arrayBuffer += item
        }
      }

      Some(arrayBuffer)
    } else {
      None
    }
  }


  /**
    * 分段，对文本按指定的分隔符分段
    *
    * @param content 输入的一行数据
    * @param sep     分隔符
    * @return 每一段为一个元素的数组
    */
  def paragraphSegment(content: String, sep: String): Array[String] = {
    val result = new ArrayBuffer[String]()
    val paragraphs = content.split(sep)
    for (paragraph <- paragraphs) {
      val filterParagraph = paragraph.trim
      if (filterParagraph != null && filterParagraph != "") {
        result += filterParagraph
      }
    }

    result.toArray
  }


  /**
    * 获取低频词
    *
    * @param wordRDD 词序列
    * @return 低频词数组
    */
  def getRareTerms(wordRDD: RDD[(Long, scala.Seq[String])]): Array[String] = {
//  def getRareTerms(wordRDD: RDD[(Long, scala.Seq[String])]): RDD[String] = {
    val rareTermNum = config.rareTermNum
    val wc = wordRDD.flatMap(words => words._2).map((_, 1)).reduceByKey(_ + _)
    val result: RDD[String] = wc.filter(word => word._2 < rareTermNum).map(word => word._1)
    result.collect()
//    result
  }


  /**
    * 删除低频词
    *
    * @param words 输入词序列
    * @param rares 低频词数组
    * @return 删除低频词后的词
    */
  def delRareTerms(id: Long, words: Seq[String], rares: Array[String]): (Long, scala.Seq[String]) = {
    val result = new ArrayBuffer[String]()
    val wordsArray = words.toArray

    for (word <- wordsArray) {
      if (!rares.contains(word)) {
        result += word
      }
    }

    (id, result)
  }


  /**
    * 判断符号是否有意义
    *
    * @param ch 输入字符
    * @return 是否有意义，如果是则返回true
    */
  private def isMeaningful(ch: Char): Boolean = {
    var result = false
    val meaningfulMarks = Array('*', '-', 'X', '.', '\\')
    if ((ch >= '一' && ch <= '龥') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || meaningfulMarks.contains(ch))
      result = true

    result
  }


  /**
    * 分词主函数
    * 执行分词，返回一个Seq[String]类型的RDD数据，分词结果不用连接符连接
    *
    * @param data 输入的一行数据
    * @return 一个元素代表一条记录
    */
  def run(data: RDD[(Long, String)]): RDD[(Long, Seq[String])] = {
    val sc = data.context
    val rdd: RDD[String] =sc.textFile(config.stopwordPath)
    val stopwordArray = sc.textFile(config.stopwordPath).collect()
    //注册
    val stopwordBC: Broadcast[Array[String]] = sc.broadcast(stopwordArray)

    //清洗数据
    val cleanedRDD = data.map(str => (str._1, baseClean(str._2)))

    //分词，去除停用词
    var resultRDD: RDD[(Long, Seq[String])] = cleanedRDD.map { line =>
      (line._1, wordSegment(line._2, stopwordBC.value))
//      (line._1, wordSegment(line._2, rdd))
    }.filter(_._2.nonEmpty).map(line => (line._1, line._2.get))
    //注销
    stopwordBC.unpersist()

    //去除低频词
    if (config.delRareTerm) {
      val rareArray = getRareTerms(resultRDD)
      resultRDD = resultRDD.map(words => delRareTerms(words._1, words._2, rareArray))
    }

    //根据词长度过滤
    resultRDD = resultRDD.map { case (id: Long, values: Seq[String]) =>
      (id, values.filter(_.length >= config.minTermSize))
    }

    resultRDD
  }

  def runSummary(data: RDD[(Long, String,String)]): RDD[(Long, Seq[String])] = {
    val sc = data.context
    //val rdd: RDD[String] =sc.textFile(config.stopwordPath)
    println(config.stopwordPath)
    val stopwordArray = sc.textFile(config.stopwordPath).collect()
    //val stopwordArray = sc.textFile("file:///"+config.stopwordPath).collect()
    //注册
    val stopwordBC: Broadcast[Array[String]] = sc.broadcast(stopwordArray)

    //清洗数据
    val cleanedRDD: RDD[(Long, String, String)] = data.map(str => (str._1, baseClean(str._2), baseClean(str._3)))

    //分词，去除停用词
    var resultRDD = cleanedRDD.map { line =>
      (line._1, wordSummary(line._2,line._3, stopwordBC.value))
      //            (line._1, wordSegment(line._2, rdd))
    }.filter(_._2.nonEmpty).map(line => (line._1, line._2.get))
    //注销
    stopwordBC.unpersist()

    //去除低频词
//    if (config.delRareTerm) {
//      val rareArray = getRareTerms(resultRDD)
//      resultRDD = resultRDD.map(words => delRareTerms(words._1, words._2, rareArray))
//    }

    //根据词长度过滤
    resultRDD = resultRDD.map { case (id: Long, values: Seq[String]) =>
      (id, values.map(x=>{
        val arr=x.split(" ")
        var c=""
        for (i<-arr){
          if(i.length>=config.minTermSize) c+=i+" "
        }
        c
      }))
    }

    resultRDD
  }
  def wordSummary(title: String,content:String, stopwordArray: Array[String]): Option[Seq[String]] = {
    //  def wordSegment(text: String, stopwordArray: RDD[String]): Option[Seq[String]] = {
    var arrayBuffer = ArrayBuffer[String]()
    if (title != null && title != "") {
//        val regex: String = "[【】●√、=★！《》※•◆：™，.。,\"!--;:?\'\\]]"
//        val segmenter: JiebaSegmenter =new JiebaSegmenter
//        val words: util.List[String] =segmenter.sentenceProcess((title+content).replaceAll(regex,"").replaceAll("[\\p{Punct}\\pP]", ""))
      var num=0
      if(content.length>=2000) num=150 else if(content.length>=800&&content.length<2000) num=100 else num=50
      val sentenceList = HanLP.extractSummary(content, num)
      val  sb=sentenceList.toArray().mkString("。")
//      val keywordList: util.List[String] = HanLP.extractKeyword( title+" "+sb,5)
      //val words: util.List[String] =HanLP.extractKeyword(title+" "+sb,10)
//      HanLP.Config.IOAdapter = new HadoopFileIoAdapter
//      val segment=new CRFSegment
//      val termList: util.List[common.Term] =segment.seg(title+content)
      //val word: String =words.toArray().mkString(" ")
      val termList: util.List[common.Term] =NLPTokenizer.segment(title+"。"+sb)
      val keywordList: Array[String] =termList.toArray.map(x=>x.toString.split("/")).filter(x=>{PARAMS.contains(x(1))}).map(x=>x(0))

      val  sbb =keywordList.toArray.filter(x=>{!stopwordArray.contains(x)}).mkString(" ").take(100)
      arrayBuffer+=sbb
      Some(arrayBuffer)
    } else {
      None
    }
  }
}

object SegmentUtils extends Logging {

  def apply(): SegmentUtils = {
    SegmentUtils("config/segment.properties")
  }

  def apply(confFile: String): SegmentUtils = {
    val config = SegmentConfig(confFile)
    new SegmentUtils(config)
  }

  def apply(prop: Properties): SegmentUtils = {
    val config = SegmentConfig(prop)
    new SegmentUtils(config)
  }

  def apply(conf: SegmentConfig): SegmentUtils = {
    new SegmentUtils(conf)
  }
}
