package test02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("pv").setMaster("local[3]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val lines = sc.textFile("D:\\ontheway\\hadoop\\spark\\spark-day02\\05\\资料\\运营商日志\\access.log")
    val length = lines.collect().length
    val line: RDD[Array[String]] = lines.map(_.split(" "))
    val ip: RDD[String] = line.map(_(0))
    val map = ip.distinct().map((_,1))
    val result = map.reduceByKey(_+_)
//    result.saveAsTextFile("D:\\ontheway\\hadoop\\spark\\spark-day02\\05\\资料\\运营商日志\\access1.log")
    println(length)
    println(result.collect().length)
  }
}
