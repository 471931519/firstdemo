package test01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN" )
    //通过并行化生成rdd
    val rdd1 = sc.parallelize(List(5, 6, 4, 7, 3, 8, 2, 9, 1, 10))
    //对rdd1里的每一个元素乘2然后排序
    val newmap: RDD[Int] = rdd1.map(_*2)
    val sort = newmap.sortBy(x=>x,false)
    //过滤出大于等于5的元素
    val filter: RDD[Int] = sort.filter(_>=5)
    //将元素以数组的方式在客户端显示
  print(filter.collect().toBuffer)
  }

}
