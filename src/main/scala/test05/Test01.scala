package test05

import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.Gson
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object Test01 {
  //获取今天日期
  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.format(now)
    return date
  }

  val maplist: mutable.HashMap[String, ListBuffer[String]] = new mutable.HashMap[String, ListBuffer[String]]()
  val listmap: ListBuffer[mutable.HashMap[String, String]] = new ListBuffer()
  var str = ""

  def main(args: Array[String]): Unit = {
//    本地运行
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
//    sc.hadoopConfiguration.set("textinputformat.record.delimiter","data_log")
    val lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\面试题\\raw_data.txt")
    //当前日期，此处用固定日期代替测试
    val words: RDD[String] = lines.flatMap(_.split("2019-02-01 "))
//判断每一条数据，放进map集合
    for (x <- words) {
      if (x.size > 10) {
        if (x.startsWith("com") || x.startsWith("tv") || x.startsWith("net")) {
          val ifhave = maplist.getOrElse(str, 0)
          if (ifhave.equals("0")) {
            val listBuffer = maplist(str)
            listBuffer.append(x)
          }
        } else if (!(x.startsWith("deviceProp") || x.startsWith("debugInfo"))) {
          val datasplit: Array[String] = x.split("data=")
          maplist.put(datasplit(0), new ListBuffer[String])
          str = datasplit(0)
          if (datasplit.length > 1) {
            if (maplist.getOrElse(str, 0) != 0) {
              val listBuffer = maplist(str)
              listBuffer.append(datasplit(1))
            }
          }
        }
      }
    }
//    遍历map集合
    for (x <- maplist) {
//      获取公共信息
      val titleinfo: Array[String] = x._1.split("&")
      val mapinfo = new mutable.HashMap[String, String]()
      for (x <- titleinfo) {
        val propinfo = x.split("=")
        if (propinfo.length == 1) {
          mapinfo.put(propinfo(0), null)
        } else {
          mapinfo.put(propinfo(0), propinfo(1))
        }
      }
//      获取每一条信息，因为具体业务不清楚，只拿了qiyi的数据，和有开始结束时间的数据
      val tvinfo = x._2
      for (x <- tvinfo) {
        val hashMap = new mutable.HashMap[String, String]
        val tvsplit = x.split("\\,\\{")
        val mininfo = tvsplit(1).split("\\,")
        if (mininfo(0).startsWith("1=2019")) {
          hashMap.put("start_time", mininfo(0).replace("1=", ""))
          hashMap.put("end_time", mininfo(1).replace("2=", ""))
          hashMap.put("program_name", "")
          hashMap.put("duration", "")
          hashMap.put("origin", "")
          hashMap.put("total_duration", "")
        } else if (mininfo(0).startsWith("1=null")) {
          hashMap.put("start_time", mininfo(4).replace("5=", ""))
          hashMap.put("end_time", mininfo(5).replace("6=", ""))
          hashMap.put("program_name", "")
          hashMap.put("duration", "")
          hashMap.put("origin", "")
          hashMap.put("total_duration", "")
        } else if (mininfo(0).startsWith("1=qiyi")) {
          hashMap.put("start_time", "")
          hashMap.put("end_time", "")
          hashMap.put("program_name", mininfo(2).replace("3=", ""))
          hashMap.put("duration", mininfo(4).replace("5=", ""))
          hashMap.put("origin", mininfo(0).replace("1=", ""))
          hashMap.put("total_duration", mininfo(3).replace("4=", ""))
        }
        if (mapinfo.getOrElse("clienttype", 0) != 0) {
          hashMap.put("client_type", mapinfo("clienttype"))
        }
        if (mapinfo.getOrElse("macline", 0) != 0) {
          hashMap.put("mac_line", mapinfo("macline"))
        }
        if (mapinfo.getOrElse("version", 0) != 0) {
          hashMap.put("version", mapinfo("version"))
        }
        hashMap.put("dt", "2019-02-01")
        hashMap.put("package_name", tvsplit(0))
        listmap.append(hashMap)
      }
      //打印
      for (x <- listmap){
        println("========")
        x.foreach(println)
      }
    }
    //存储本地
    val result: RDD[mutable.HashMap[String, String]] = sc.parallelize(listmap)
    result.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\面试题\\a.txt")

  }
}
