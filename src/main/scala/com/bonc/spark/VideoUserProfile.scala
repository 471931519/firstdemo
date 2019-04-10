package com.bonc.spark

import java.util
import java.util.{Date, HashMap => JHashMap}

import com.alibaba.fastjson.JSON
import com.bonc.constants.Constants
import com.bonc.utils.{CalculateUtils, DateUtils}
import com.ifeng.mnews.basesys.wechat.WxService
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: toby
  * Description: video视频用户画像主程序
  * Create: 2018-10-26
  */
object VideoUserProfile {
  def main(args: Array[String]): Unit = {
    try {
      //      if (args.length != 5) {
      //        System.err.println("Please Input the Input File's Path!")
      //        System.exit(0)
      //      }
      //不使用配置文件，也不使用kafka数据源
      //            val configFilePath = args(0)
      val dc_videoclickPath = args(1)
      val dc_videoinviewPath = args(2)
      val video_proilePath = args(3)
      val hdfs_path = args(4)

      val conf = new SparkConf()
      conf
        .setAppName("Video_UserProfile")
      //        .setMaster("local[2]")
      val sc = new SparkContext(conf)
      val spark = SparkSession.builder().config(conf).getOrCreate()

      //获取RDD
      val videoRdd = getFromHDFS(video_proilePath,sc)
      println("videoRdd数量为" + videoRdd.count)
      val clickRdd = getFromHDFS(dc_videoclickPath, sc)
      println("clickRdd数量为" + clickRdd.count)
      val inviewRdd = getFromHDFS(dc_videoinviewPath, sc)
      println("inviewRdd数量为" + inviewRdd.count)

      //解析成JSON对象
      val newsJSONObjectRdd = videoRdd.map(JSON.parseObject(_))
      //    val newsFilterJSONObjectRdd = CalculateUtils.filterNewsRdd(newsJSONObjectRdd) 不要在外面过滤，丢数据
      val newsFilterJSONObjectRdd = newsJSONObjectRdd.repartition(1000)
      val clickJSONObjectRdd = clickRdd.map(JSON.parseObject(_))
      val inviewJSONObjectRdd = inviewRdd.map(JSON.parseObject(_))

      newsFilterJSONObjectRdd.cache()

      //新闻维度flatMap拆分，click/inview数据形式改为 newsId + ":" + deviceId
      val newsTagRdd: RDD[String] = CalculateUtils.getTargetNewsRdd(newsFilterJSONObjectRdd, Constants.TARGET_TAG_JSON_NAME)
      val newsOuterTagRdd: RDD[String] = CalculateUtils.getTargetNewsRdd(newsFilterJSONObjectRdd, Constants.TARGET_OUTERTAG_JSON_NAME)
      val newsOuterCateRdd: RDD[String] = CalculateUtils.getTargetNewsRdd(newsFilterJSONObjectRdd, Constants.TARGET_OUTERCATE_JSON_NAME)
      val newsDfromRdd: RDD[String] = CalculateUtils.getDFromRdd(newsFilterJSONObjectRdd, Constants.TARGET_DFROM_JSON_NAME)

      val clickNewsDeviceRdd = CalculateUtils.getNewsDeviceRdd(clickJSONObjectRdd, spark)
      clickNewsDeviceRdd.cache()
      val inviewNewsDeviceRdd = CalculateUtils.getNewsDeviceRdd(inviewJSONObjectRdd, spark)
      inviewNewsDeviceRdd.cache()

      //RDD注册成表
      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_TAG_TABLE_NAME, Constants.NEWS_TAG_TABLE_NAME, newsTagRdd, spark)
      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_OUTERTAG_TABLE_NAME, Constants.NEWS_OUTERTAG_TABLE_NAME, newsOuterTagRdd, spark)
      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_OUTERCATE_TABLE_NAME, Constants.NEWS_OUTERCATE_TABLE_NAME, newsOuterCateRdd, spark)
      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_DFROM_TABLE_NAME, Constants.NEWS_DFROM_TABLE_NAME, newsDfromRdd, spark)

      CalculateUtils.registerScoreTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_DEVICEID_TABLE_NAME, Constants.TARGET_SCORE_TABLE_NAME, Constants.BASIC_CLICK_TABLE_NAME, clickNewsDeviceRdd, spark)
      CalculateUtils.registerScoreTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_DEVICEID_TABLE_NAME, Constants.TARGET_SCORE_TABLE_NAME, Constants.BASIC_INVIEW_TABLE_NAME, inviewNewsDeviceRdd, spark)

      //用户点击/曝光 与新闻维度进行关联
      val clickDeviceTag = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_TAG_TABLE_NAME, Constants.TARGET_TAG_TABLE_NAME, spark)
      val clickDeviceOutertag = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_OUTERTAG_TABLE_NAME, Constants.TARGET_OUTERTAG_TABLE_NAME, spark)
      val clickDeviceOutercate = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_OUTERCATE_TABLE_NAME, Constants.TARGET_OUTERCATE_TABLE_NAME, spark)
      val clickDeviceDfrom = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_DFROM_TABLE_NAME, Constants.TARGET_DFROM_TABLE_NAME, spark)

      val inviewDeviceTag = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_TAG_TABLE_NAME, Constants.TARGET_TAG_TABLE_NAME, spark)
      val inviewDeviceOutertag = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_OUTERTAG_TABLE_NAME, Constants.TARGET_OUTERTAG_TABLE_NAME, spark)
      val inviewDeviceOutercate = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_OUTERCATE_TABLE_NAME, Constants.TARGET_OUTERCATE_TABLE_NAME, spark)
      val inviewDeviceDfrom = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_DFROM_TABLE_NAME, Constants.TARGET_DFROM_TABLE_NAME, spark)


      clickDeviceTag.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_TAG_TABLE_NAME)
      clickDeviceOutertag.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_OUTERTAG_TABLE_NAME)
      clickDeviceOutercate.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_OUTERCATE_TABLE_NAME)
      clickDeviceDfrom.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_DFROM_TABLE_NAME)
      inviewDeviceTag.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_TAG_TABLE_NAME)
      inviewDeviceOutertag.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_OUTERTAG_TABLE_NAME)
      inviewDeviceOutercate.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_OUTERCATE_TABLE_NAME)
      inviewDeviceDfrom.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_DFROM_TABLE_NAME)

      val tagResultRdd = CalculateUtils.getVideoTargetResult(Constants.JOIN_CLICK_DEVICE_TAG_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_TAG_TABLE_NAME, Constants.TARGET_TAG_TABLE_NAME, Constants.TARGET_TAG_JSON_NAME, spark)
      val outertagResultRdd = CalculateUtils.getVideoTargetResult(Constants.JOIN_CLICK_DEVICE_OUTERTAG_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_OUTERTAG_TABLE_NAME, Constants.TARGET_OUTERTAG_TABLE_NAME, Constants.TARGET_OUTERTAG_JSON_NAME, spark)
      val outercatetagResultRdd = CalculateUtils.getVideoTargetResult(Constants.JOIN_CLICK_DEVICE_OUTERCATE_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_OUTERCATE_TABLE_NAME, Constants.TARGET_OUTERCATE_TABLE_NAME, Constants.TARGET_OUTERCATE_JSON_NAME, spark)
      val dfromResultRdd = CalculateUtils.getVideoTargetResult(Constants.JOIN_CLICK_DEVICE_DFROM_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_DFROM_TABLE_NAME, Constants.TARGET_DFROM_TABLE_NAME, Constants.TARGET_DFROM_JSON_NAME, spark)

      println("tagResultRdd数量为" + tagResultRdd.count)
      println("outertagResultRdd数量为" + outertagResultRdd.count)
      println("outercatetagResultRdd数量为" + outercatetagResultRdd.count)
      println("dfromResultRdd数量为" + dfromResultRdd.count)


      val unionResultRdd = tagResultRdd
        .union(outertagResultRdd)
        .union(outercatetagResultRdd)
        .union(dfromResultRdd)

      val clickRDD = spark.sql(
        s"""
           |select ${Constants.TARGET_DEVICEID_TABLE_NAME},count(distinct ${Constants.TARGET_NEWSID_TABLE_NAME})
           |from ${Constants.BASIC_CLICK_TABLE_NAME}
           |group by ${Constants.TARGET_DEVICEID_TABLE_NAME}
  """.stripMargin)
        .rdd
        .map(x => {
          val deviceid = x.get(0).toString
          val pv = "click_pv_stat:" + x.get(1).toString
          (deviceid, pv)
        })
      println("clickRDD数量为" + clickRDD.count)

      val inviewRDD = spark.sql(
        s"""
           |select ${Constants.TARGET_DEVICEID_TABLE_NAME},count(distinct ${Constants.TARGET_NEWSID_TABLE_NAME})
           |from ${Constants.BASIC_INVIEW_TABLE_NAME}
           |group by ${Constants.TARGET_DEVICEID_TABLE_NAME}
  """.stripMargin)
        .rdd
        .map(x => {
          val deviceid = x.get(0).toString
          val pv = "inview_pv_stat:" + x.get(1).toString
          (deviceid, pv)
        })
      println("inviewRDD数量为" + inviewRDD.count)

      val finalRdd = CalculateUtils.getFinalResultRdd(unionResultRdd, clickRDD, inviewRDD)
      println("finalRdd数量为" + finalRdd.count)

      finalRdd.repartition(10).saveAsTextFile(hdfs_path)
      //      finalRdd.foreach(println)
    } catch {
      case e => e.printStackTrace()
        val wxService = new WxService
        val text = new util.HashMap[String, String]()
        text.put("content", "ERROR!!! in UserProfile,please check" + new Date())
        val result = wxService.sendMessage(1, "sutuo@qknode.com", "text", "monitor", text, "secret")
    }
  }


  def getFromHDFS(path: String, sc: SparkContext): RDD[String] = {
    var arr = path.dropRight(1).split(",")
    var rdd: RDD[String] = sc.textFile(arr.head)
    for (i <- arr.tail) {
      rdd = rdd.union(sc.textFile(i))
    }
    rdd
  }
}
