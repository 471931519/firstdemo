package test03

import org.apache.spark.sql.SparkSession

object HiveSupport {
  def main(args: Array[String]): Unit = {
    //todo:1、创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("HiveSupport")
//      .master("spark://node01:7077")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "d:\\newspace\\spark\\spark-warehouse")
      .enableHiveSupport() //开启支持hive
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN") //设置日志输出级别
    spark.sql("CREATE TABLE IF NOT EXISTS person (id int, name string, age int) row format delimited fields terminated by ' '")
    spark.sql("LOAD DATA LOCAL INPATH './date/person.txt' INTO TABLE person")
    spark.sql("select * from person ").show()
//    println(spark.sql("show databases"))
    spark.stop()

  }
}
