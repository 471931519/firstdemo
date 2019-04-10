package test05

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Test02 {
  def main(args: Array[String]): Unit = {
    //    本地运行
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc.hadoopConfiguration.set("textinputformat.record.delimiter","data_log")
    val lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\面试题\\raw_data.txt")
    //当前日期，此处用固定日期代替测试
//  val words: RDD[String] = lines.map(_.replace("",""))
//    val strings = words.collect()
//    strings.foreach(println)
    lines.foreach(println)
  }
}
