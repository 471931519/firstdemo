package com.bonc.utils

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}

import scala.collection.JavaConversions._

/**
  * Author: YangYunhe
  * Description: 
  * Create: 2018-06-29 11:35
  */
object SparkKafkaUtils {

  /**
    * 从 Kafka 中取数据加载到 RDD 中
    * @param sc SparkContext
    * @param topic Kafka 的 Topic
    * @param numDays 取距离此刻多少天之前的数据，例如，这个参数为 3，那么取此刻和3天之前相同时刻范围内的数据
    * @param kafkaParams Kafka的配置参数，用于创建生产者和作为参数传给 KafkaUtils.createRDD
    * @return
    */
  def createKafkaRDDByTimeRange(sc: SparkContext, topic: String, numDays: Int, kafkaParams: java.util.HashMap[String, Object]): RDD[String] = {

    val startFetchTime = DateUtils.daysAgo(numDays)
    val startFetchTimeStr = DateUtils.parseLong2String(startFetchTime, DateUtils.DATE_TIME_FORMAT_STR)
    println(s"starting fetch data in kafka with time range [${startFetchTimeStr}——${DateUtils.nowStr()}]")

    val consumer = new KafkaConsumer[String, String](kafkaParams)

    val partitionInfos = consumer.partitionsFor(topic)
    val topicPartitions = scala.collection.mutable.ArrayBuffer[TopicPartition]()
    val timestampsToSearch = scala.collection.mutable.Map[TopicPartition, java.lang.Long]()
    val offsetRanges = scala.collection.mutable.ArrayBuffer[OffsetRange]()

    for(partitionInfo <- partitionInfos) {
      topicPartitions += new TopicPartition(partitionInfo.topic, partitionInfo.partition)
    }

    val topicPartitionLongMap = consumer.endOffsets(topicPartitions)

    for(topicPartition <- topicPartitions) {
      timestampsToSearch(topicPartition) = startFetchTime
      println(topicPartition+":"+startFetchTime)
    }

    val  topicPartitionOffsetAndTimestampMap= consumer.offsetsForTimes(timestampsToSearch)
    println(topicPartitionOffsetAndTimestampMap.size==0)
    for((k, v) <- topicPartitionOffsetAndTimestampMap) {
      println(k.partition()+"..."+v.offset())
      offsetRanges += OffsetRange.create(topic, k.partition(), v.offset(), topicPartitionLongMap.get(k))
    }

    KafkaUtils.createRDD[String, String](sc, kafkaParams, offsetRanges.toArray, PreferConsistent).map(_.value)

  }


}
