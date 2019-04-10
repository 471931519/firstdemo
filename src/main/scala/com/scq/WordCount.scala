package com.scq

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //spark配置文件
    val conf: SparkConf = new SparkConf().setAppName("Wordcount").setMaster("local[2]")
    //spark上下文对象
    val context: SparkContext = new SparkContext(conf)
    context.setLogLevel("WARN")
    //获得每一行
    val lines: RDD[String] = context.textFile("D:/test/test/b.txt")
    //获得没个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //获得map
    val word: RDD[(String, Int)] = words.map((_,1))
//    获得计数map
    val result: RDD[(String, Int)] = word.reduceByKey(_+_)
    val newresult = result.sortBy(_._2,true)
    println(newresult.collect().toBuffer)
  }
}
