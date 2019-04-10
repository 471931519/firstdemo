package sparkstream.test01

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test03 {

def myfun(function: (Int, Int) => Int , i: Int, i1: Int)={
    i+i1
}
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test03").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val stream: StreamingContext = new StreamingContext(sc,Seconds(5))
    val lines: ReceiverInputDStream[String] = stream.socketTextStream("192.168.40.100",9999)
    val line: DStream[String] = lines.flatMap(_.split(" "))
    val words: DStream[(String, Int)] = line.map((_,1))
    val result = words.reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(5),Seconds(10))
    result.print()
    stream.start()
    stream.awaitTermination()
    println()
  }
}
