package test01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val lines = sc.textFile("D:\\test\\test\\b.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    words.repartition(3)
    val word: RDD[(String, Int)] = words.map((_,1))
    val result: RDD[(String, Int)] = word.reduceByKey(_+_)
    println(result.collect().toBuffer)
    sc.textFile("/1.log").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

  }
}
