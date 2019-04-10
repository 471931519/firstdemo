package sparkstream.test01.test02

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object Test01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test01").setMaster("local[4]").set("spark.streaming.receiver.writeAheadLog.enable","true")
    val sc: SparkContext =new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("./ckspark_kafka")
    val zkQuorum="node01:2181,node02:2181,node03:2181"
    //5、定义消费者组
    val groupId="spark_receiver1"
    val topics=Map("mytest" -> 2)
    val receiverDstream: IndexedSeq[ReceiverInputDStream[(String, String)]] = (1 to 3).map(x => {
      val stream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)
      stream
    }
    )
    val unionDStream: DStream[(String, String)] = ssc.union(receiverDstream)

    //8、获取topic中的数据
    val topicData: DStream[String] = unionDStream.map(_._2)
    //9、切分每一行,每个单词计为1
    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_,1))
    //10、相同单词出现的次数累加
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //11、打印输出
    result.print()
    //开启计算
    ssc.start()
    ssc.awaitTermination()
    print()
  }


}
