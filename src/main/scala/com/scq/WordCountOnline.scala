package com.scq

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountOnline {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wordcountOnline")
    val sc: SparkContext = new SparkContext(conf)
//    去日志
    //获取文件
    val lines: RDD[String] = sc.textFile(args(0))
//    获取每个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

//    获取单词map
    val word: RDD[(String, Int)] = words.map((_,1))
//    获取单词计数
    val result: RDD[(String, Int)] = word.reduceByKey(_+_)

    val newresult = result.sortBy(_._2,true)

    newresult.saveAsTextFile("/spark_test")
    sc.stop()
  }
}
