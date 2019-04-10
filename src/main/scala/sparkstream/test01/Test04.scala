package sparkstream.test01

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test04 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test04").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val stream = new StreamingContext(sc, Seconds(5))
    val lines: ReceiverInputDStream[String] = stream.socketTextStream("192.168.40.100", 9999)
    val line: DStream[String] = lines.flatMap(_.split(" "))
    val words = line.map((_, 1))
    val reslut: DStream[(String, Int)] = words.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(5), Seconds(5))
    val sortby: DStream[(String, Int)] = reslut.transform(rdd => {
      val sort = rdd.sortBy(_._2, false)
      val take: Array[(String, Int)] = sort.take(3)
      println("-------start--------")
      take.foreach(x=>{println(x)})
      println("--------end-------")
      sort
    })
    sortby.print()
    stream.start()
    stream.awaitTermination()
  }

}
