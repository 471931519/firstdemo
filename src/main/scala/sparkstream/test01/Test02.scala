package sparkstream.test01

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test02 {


  def updateFunc(newvalue:Seq[Int], runningcount:Option[Int]):Option[Int] = {
    val newCount =runningcount.getOrElse(0)+newvalue.sum
    Some(newCount)

  }


  def main(args: Array[String]): Unit = {
    val conf: SparkConf =new SparkConf().setAppName("test02").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val stream: StreamingContext = new StreamingContext(sc,Seconds(5))
    stream.checkpoint("./chkstream")
    val input: ReceiverInputDStream[String] = stream.socketTextStream("192.168.40.100",9999)
    val line: DStream[String] = input.flatMap(_.split(" "))
    val words: DStream[(String, Int)] = line.map((_,1))
    val result: DStream[(String, Int)] = words.updateStateByKey(updateFunc)
    result.print()
    stream.start()
    stream.awaitTermination()
  }
}
