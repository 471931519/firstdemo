package test04

import java.util.Properties

import org.apache.spark.sql.SparkSession

object DataFromSql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("datasql").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    val dataFrame = spark.read.jdbc("jdbc:mysql://192.168.40.120:3306/test","students",properties)
    dataFrame.show()
    sc.stop()

  }
}
