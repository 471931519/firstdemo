package sparkstream.test01

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test05 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Terst05").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val stream: StreamingContext = new StreamingContext(sc,Seconds(5))
    val lines: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(stream,"192.168.40.100",8888)
    val date: DStream[String] = lines.map(x=>new String(x.event.getBody.array()))
    val line = date.flatMap(_.split(" "))
    val words: DStream[(String, Int)] = line.map((_,1))
    val result: DStream[(String, Int)] = words.reduceByKeyAndWindow((x:Int, y:Int)=>x+y,Seconds(5),Seconds(5))
    result.print()
    stream.start()
    stream.awaitTermination()
  }
}
