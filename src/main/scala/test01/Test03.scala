package test01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //    val rdd1 = sc.parallelize(Array("a b c", "d e f", "h i j"))
    //    //将rdd1里面的每一个元素先切分在压平
    //    val map: RDD[String] = rdd1.flatMap(_.split(" "))
    //    println(map.collect().toBuffer)

    /*
    val rdd1 = sc.parallelize(List(5, 6, 4, 3))
    val rdd2 = sc.parallelize(List(1, 2, 3, 4))
    //求并集
    val union1: RDD[Int] = rdd1.union(rdd2)
    //求交集
    val union2: RDD[Int] = rdd1.intersection(rdd2)
    //去重
    println(union1.distinct().collect().toBuffer)
    println(union2.collect().toBuffer)*/
   /* val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 3), ("kitty", 2)))
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))
    //求join
    val join = rdd1.join(rdd2)
    //求并集
    val union = rdd1.union(rdd2)
    println(join)
    println(union)
    //按key进行分组

*/

    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))
    //reduce聚合
      val i: Int = rdd1.reduce(_+_)
    println(i)
//    练习7：reduceByKey、sortByKey
    val rdd2 = sc.parallelize(List(("tom", 1), ("jerry", 3), ("kitty", 2),  ("shuke", 1)))
    val rdd3 = sc.parallelize(List(("jerry", 2), ("tom", 3), ("shuke", 2), ("kitty", 5)))
    //按key进行聚合
    val rdd4: RDD[(String, Int)] = rdd2.union(rdd3)
    //按value的降序排序
    val sort: RDD[(String, Int)] = rdd4.sortBy(_._2,false)
    println(sort.collect().toBuffer)
//    练习8：repartition、coalesce
    //利用repartition改变rdd1分区数
    //减少分区
    //增加分区
    //利用coalesce改变rdd1分区数
    //减少分区



  }
}
