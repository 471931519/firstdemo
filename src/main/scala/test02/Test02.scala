package test02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test02 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("pv").setMaster("local[3]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val lines = sc.textFile("D:\\ontheway\\hadoop\\spark\\spark-day02\\05\\资料\\运营商日志\\access.log")
    val line: RDD[Array[String]] = lines.map(_.split(" "))
    val value: RDD[String] = line.filter(_.length>10).map(_(10))
    val map = value.filter(!_.equals("\"-\""))map((_,1))
    val result = map.reduceByKey(_+_)
    val sort = result.sortBy(_._2,false)
    val array: Array[(String, Int)] = sort.take(5)
    println(array.toBuffer)

    val stringses: Array[Array[String]] = value.collect().map(_.split("/"))
    val strings: Array[String] = stringses(5)
    println(array.toBuffer)
  }
}
