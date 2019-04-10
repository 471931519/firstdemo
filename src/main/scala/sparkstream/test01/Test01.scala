package sparkstream.test01

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test01").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val streaming: StreamingContext = new StreamingContext(sc,Seconds(5))
    val wordAndOne: ReceiverInputDStream[String] = streaming.socketTextStream("192.168.40.100",9999)
    val line: DStream[String] = wordAndOne.flatMap(_.split(" "))
    val words: DStream[(String, Int)] = line.map((_,1))
    val result: DStream[(String, Int)] = words.reduceByKey(_+_)
    result.print()

    streaming.start()
    streaming.awaitTermination()
  }
}
