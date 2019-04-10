package test03

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//case class Person(id:Int,name:String,age:Int)
object CaseClassSchema {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("classSchema").master("local[3]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    val lines: RDD[String] = sc.textFile("D:\\ontheway\\hadoop\\spark\\spark-day03\\06\\资料\\person.txt")
    val line: RDD[Array[String]] = lines.map(_.split(" "))
    //todo 这是第一种方式RDD转换成DataFarame
//    val personline: RDD[Person] = line.map(x=>(Person(x(0).toInt,x(1),x(2).toInt)))
//    import spark.implicits._
//    val personDF = personline.toDF()

//    personDF.show()
//    personDF.printSchema()
//    println(personDF.count())
//    personDF.columns.foreach(println)
//    personDF.filter($"age">30).show()
    //todo----------------------------------

    //todo 这是第二种方式
        val personline: RDD[Row] = line.map(x=>Row(x(0).toInt,x(1),x(2).toInt))
    val schema: StructType = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("age", IntegerType, false)
    ))
    val personDF: DataFrame = spark.createDataFrame(personline,schema)
    personDF.show()

  }
}
