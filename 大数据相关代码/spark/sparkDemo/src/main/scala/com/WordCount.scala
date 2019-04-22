
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val input = sc.textFile("/tmp/wc.txt")
    val lines = input.flatMap(line => line.split(" "))
    val count = lines.map(word => (word, 1)).reduceByKey(_+_)
    count.foreach(x=>{
      println(x)
    })
    count.saveAsTextFile("/tmp/output")
  }

}