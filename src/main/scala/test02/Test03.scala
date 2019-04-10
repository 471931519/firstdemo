package test02

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("pv").setMaster("local[3]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //对比文件
    val lines: RDD[String] = sc.textFile("D:\\ontheway\\hadoop\\spark\\spark-day02\\05\\资料\\服务器访问日志根据ip地址查找区域\\ip.txt")
    val line: RDD[Array[String]] = lines.map(_.split("\\|"))
    val info: RDD[(Long, Long, String, String, String)] = line.map(x => (x(2).toLong, x(3).toLong, x(7), x(13), x(14)))
    val newline: Broadcast[Array[(Long, Long, String, String, String)]] = sc.broadcast(info.collect())
    //获得了所有地理信息  ip，ip，城市，x，y
    val newInfo: Array[(Long, Long, String, String, String)] = newline.value

    //    目标文件
    val newlines: RDD[String] = sc.textFile("D:\\ontheway\\hadoop\\spark\\spark-day02\\05\\资料\\服务器访问日志根据ip地址查找区域\\20090121000132.394251.http.format")
    val userips: RDD[Array[String]] = newlines.map(_.split("\\|"))
    val ips: RDD[String] = userips.map(_ (1))
    val ip: RDD[Long] = ips.map(x => (ipToLong(x)))

    val index: RDD[Int] = ip.map(x=>(binarySearch(x,newInfo)))
    val result: RDD[(String, String, String)] = index.map(x=>(newInfo(x)._3,newInfo(x)._4,newInfo(x)._5))
    val sort: RDD[(String, String, String)] = result.sortBy(_._1)
    val xymap: RDD[((String, String, String), Int)] = result.map((_,1))
    val reduce: RDD[((String, String, String), Int)] = xymap.reduceByKey(_+_)


    println(result.collect().toBuffer)
  }

  def ipToLong(ip: String): Long = {
    //todo：切分ip地址。
    val ipArray: Array[String] = ip.split("\\.")
    var ipNum = 0L

    for (i <- ipArray) {
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(ip: Long, arrary: Array[(Long, Long, String, String, String)]): Int = {
    var start = 0
    var end = arrary.length - 1
    while(start<end){
      val middle=(start+end)/2

      if (ip>=arrary(middle)._1&&ip<=arrary(middle)._2){
        return middle
      }
      if (ip>arrary(middle)._2.toLong){
        start=middle
      }
      if (ip<arrary(middle)._1.toLong){
        end=middle
      }

    }

    -1
  }

}
