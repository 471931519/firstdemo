package test04

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object SparkSqlToMysql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val lines: RDD[String] = sc.textFile(args(0))
    val line: RDD[Array[String]] = lines.map(_.split(" "))
    val stu: RDD[Student] = line.map(x=>Student(x(0).toInt,x(1),x(2).toInt))
    import spark.implicits._
    val stuDF: DataFrame = stu.toDF()
    stuDF.createOrReplaceTempView("student")
    val resultDF: DataFrame = spark.sql("select * from student order by age desc")

    val prop =new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")

    resultDF.write.jdbc("jdbc:mysql://192.168.40.120:3306/test","student",prop)
    resultDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.40.120:3306/test","student",prop)
    spark.stop()


  }

}
case class Student(id:Int,name:String,age:Int)
