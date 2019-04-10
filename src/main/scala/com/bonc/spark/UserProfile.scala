package com.bonc.spark

import java.io.{File, FileInputStream}
import java.util
import java.util.{Date, Properties, HashMap => JHashMap}

import com.alibaba.fastjson.JSON
import com.bonc.constants.Constants
import com.bonc.utils.{CalculateUtils, DateUtils, SparkKafkaUtils}
import com.ifeng.mnews.basesys.wechat.WxService
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
  * Author: YangYunhe，toby
  * Description: 用户画像主程序
  * 调整后tag取前500
  * 添加categoryTag交叉；添加兴趣衰减系；添加新闻点击，曝光；添加wilson系数0.95
  * 当前ck使用系数为0.1
  * Create: 2018-06-29 11:33
  */
object UserProfile {
  def main(args: Array[String]): Unit = {
    try {
      //      if (args.length != 5) {
      //        System.err.println("Please Input the Input File's Path!")
      //        System.exit(0)
      //      }
      //不使用配置文件，也不使用kafka数据源
      val configFilePath = args(0)
      val dc_clickPath = args(1)
      val dc_inviewPath = args(2)
      val news_proilePath = args(3)
      val hdfs_path = args(4)
      val yesterday = args(5)
      //      val dc_clickPath = "file:///D:\\software\\IDEA\\project\\user-profile\\src\\main\\resources\\userprofile\\click.txt1"
      //      val dc_inviewPath = "file:///D:\\software\\IDEA\\project\\user-profile\\src\\main\\resources\\userprofile\\inview.txt1"
      //      val news_proilePath = "file:///D:\\software\\IDEA\\project\\user-profile\\src\\main\\resources\\userprofile\\profile.txt1"
      //      val yesterday = "20181117"

      //      val dc_clickPath = "file:///D:\\tmp\\userprofile\\dc_clickpath.txt,"
      //      val dc_inviewPath = "file:///D:\\tmp\\userprofile\\dc_inviewpath.txt,"
      //      val news_proilePath = "file:///D:\\tmp\\userprofile\\hdfs_path.txt,"
      //      val hdfs_path = "file:///D:\\tmp\\userprofile\\news_profilepath.txt,"


      val now = DateUtils.now()
      val nowDateStr = DateUtils.parseLong2String(now, DateUtils.DATE_FORMAT_STR)

      //      val props = new Properties()
      //      val in = new FileInputStream(new File(configFilePath))
      //      props.load(in)

      //      val bootstrapServers = props.getProperty("bootstrap.servers")
      //      val group_id = props.getProperty("group.id")
      //      val newsProfileTopic = props.getProperty("news.topic.name")
      //      val newsProfileTopicDays = props.getProperty("news.topic.days.num")
      //      val clickTopic = props.getProperty("click.topic.name")
      //      val clickTopicDays = props.getProperty("click.topic.days.num")
      //      val inviewTopic = props.getProperty("inview.topic.name")
      //      val inviewTopicDays = props.getProperty("inview.topic.days.num")
      //      val hdfsPath = props.getProperty("output.hdfs.path")


      val spark = SparkSession
        .builder()
        //        .master("local[2]")
        .appName(s"${this.getClass.getSimpleName}")
        .enableHiveSupport()
        .getOrCreate()

      val sc = spark.sparkContext
      //      val kafkaParams = new JHashMap[String, Object]()
      //      kafkaParams.put("bootstrap.servers", bootstrapServers)
      //      kafkaParams.put("group.id", group_id)
      //      kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      //      kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

      //    val newsRdd = SparkKafkaUtils.createKafkaRDDByTimeRange(sc, newsProfileTopic, newsProfileTopicDays.toInt, kafkaParams)
      //    val clickRdd = SparkKafkaUtils.createKafkaRDDByTimeRange(sc, clickTopic, clickTopicDays.toInt, kafkaParams)
      //    val inviewRdd = SparkKafkaUtils.createKafkaRDDByTimeRange(sc, inviewTopic, inviewTopicDays.toInt, kafkaParams)
      //获取RDD
      val newsRdd = getFromHDFS(news_proilePath, sc)
      //      println("newsRdd数量为" + newsRdd.count)
      val clickRdd = getFromHDFS(dc_clickPath, sc)
      //      println("clickRdd数量为" + clickRdd.count)
      val inviewRdd = getFromHDFS(dc_inviewPath, sc)
      //      println("inviewRdd数量为" + inviewRdd.count)

      //解析成JSON对象
      val newsJSONObjectRdd = newsRdd.map(x=>{
        try {
          JSON.parseObject(x)
        } catch {
          case e =>e.printStackTrace()
            println(x)
            val not_json ="""{'null':'null'}"""
            JSON.parseObject(not_json)
        }
      }
      )
      //    val newsFilterJSONObjectRdd = CalculateUtils.filterNewsRdd(newsJSONObjectRdd) 不要在外面过滤，丢数据
      val newsFilterJSONObjectRdd = newsJSONObjectRdd.repartition(1000)
      val clickJSONObjectRdd = clickRdd.map(JSON.parseObject(_))
      val inviewJSONObjectRdd = inviewRdd.map(JSON.parseObject(_))

      newsFilterJSONObjectRdd.cache()

      //新闻维度flat 2Map拆分，click/inview数据形式改为 newsId + ":" + deviceId
      val newsCategoryRdd: RDD[String] = CalculateUtils.getTargetNewsRdd(newsFilterJSONObjectRdd, Constants.TARGET_CATEGORY_JSON_NAME)
      val newsSecondCateRdd: RDD[String] = CalculateUtils.getTargetNewsRdd(newsFilterJSONObjectRdd, Constants.TARGET_SECOND_CATE_JSON_NAME)
      val newsTagRdd: RDD[String] = CalculateUtils.getTargetNewsRdd(newsFilterJSONObjectRdd, Constants.TARGET_TAG_JSON_NAME)
      val newsDfromRdd: RDD[String] = CalculateUtils.getDFromRdd(newsFilterJSONObjectRdd, Constants.TARGET_DFROM_JSON_NAME)
      val newsCategoryDfromRdd: RDD[String] = CalculateUtils.getCategoryDfromNewsRdd(newsFilterJSONObjectRdd)
      val newsCategoryTagRdd: RDD[String] = CalculateUtils.getCategoryTagNewsRdd(newsFilterJSONObjectRdd)
      //      val newsT64Rdd: RDD[String] = CalculateUtils.getTargetNewsRdd(newsFilterJSONObjectRdd, Constants.TARGET_T64_JSON_NAME)
      //      val newsT128Rdd: RDD[String] = CalculateUtils.getTargetNewsRdd(newsFilterJSONObjectRdd, Constants.TARGET_T128_JSON_NAME)
      //      val newsT256Rdd: RDD[String] = CalculateUtils.getTargetNewsRdd(newsFilterJSONObjectRdd, Constants.TARGET_T256_JSON_NAME)
      //      val newsT512Rdd: RDD[String] = CalculateUtils.getTargetNewsRdd(newsFilterJSONObjectRdd, Constants.TARGET_T512_JSON_NAME)
      //      val newsT1000Rdd: RDD[String] = CalculateUtils.getTargetNewsRdd(newsFilterJSONObjectRdd, Constants.TARGET_T1000_JSON_NAME)
      val clickNewsDeviceRdd = CalculateUtils.getNewsDeviceRdd(clickJSONObjectRdd, spark)
      clickNewsDeviceRdd.cache()
      val inviewNewsDeviceRdd = CalculateUtils.getNewsDeviceRdd(inviewJSONObjectRdd, spark)
      inviewNewsDeviceRdd.cache()
      //      println("newsCategoryRdd数量为" + newsCategoryRdd.count)
      //      println("newsSecondCateRdd数量为" + newsSecondCateRdd.count)
      //      println("newsTagRdd数量为" + newsTagRdd.count)
      //      println("newsDfromRdd数量为" + newsDfromRdd.count)
      //      println("newsCategoryDfromRdd数量为" + newsCategoryDfromRdd.count)
      //      println("newsCategoryTagRdd数量为" + newsCategoryTagRdd.count)
      //      println("clickNewsDeviceRdd数量为" + clickNewsDeviceRdd.count)
      //      println("inviewNewsDeviceRdd数量为" + inviewNewsDeviceRdd.count)

      //RDD注册成表
      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_CATEGORY_TABLE_NAME, Constants.NEWS_CATEGORY_TABLE_NAME, newsCategoryRdd, spark)
      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_SECOND_CATE_TABLE_NAME, Constants.NEWS_SECOND_CATE_TABLE_NAME, newsSecondCateRdd, spark)
      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_TAG_TABLE_NAME, Constants.NEWS_TAG_TABLE_NAME, newsTagRdd, spark)
      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_DFROM_TABLE_NAME, Constants.NEWS_DFROM_TABLE_NAME, newsDfromRdd, spark)
      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_CATEGORY_DFROM_TABLE_NAME, Constants.NEWS_CATEGORY_DFROM_TABLE_NAME, newsCategoryDfromRdd, spark)
      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_CATEGORY_TAG_TABLE_NAME, Constants.NEWS_CATEGORY_TAG_TABLE_NAME, newsCategoryTagRdd, spark)
      //      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_T64_TABLE_NAME, Constants.NEWS_T64_TABLE_NAME, newsT64Rdd, spark)
      //      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_T128_TABLE_NAME, Constants.NEWS_T128_TABLE_NAME, newsT128Rdd, spark)
      //      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_T256_TABLE_NAME, Constants.NEWS_T256_TABLE_NAME, newsT256Rdd, spark)
      //      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_T512_TABLE_NAME, Constants.NEWS_T512_TABLE_NAME, newsT512Rdd, spark)
      //      CalculateUtils.registerBasicTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_T1000_TABLE_NAME, Constants.NEWS_T1000_TABLE_NAME, newsT1000Rdd, spark)
      CalculateUtils.registerScoreTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_DEVICEID_TABLE_NAME, Constants.TARGET_SCORE_TABLE_NAME, Constants.BASIC_CLICK_TABLE_NAME, clickNewsDeviceRdd, spark)
      CalculateUtils.registerScoreTable(Constants.TARGET_NEWSID_TABLE_NAME, Constants.TARGET_DEVICEID_TABLE_NAME, Constants.TARGET_SCORE_TABLE_NAME, Constants.BASIC_INVIEW_TABLE_NAME, inviewNewsDeviceRdd, spark)


      //用户点击/曝光 与新闻维度进行关联
      val clickDeviceCategory = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_CATEGORY_TABLE_NAME, Constants.TARGET_CATEGORY_TABLE_NAME, spark)
      val clickDeviceSecondCate = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_SECOND_CATE_TABLE_NAME, Constants.TARGET_SECOND_CATE_TABLE_NAME, spark)
      val clickDeviceTag = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_TAG_TABLE_NAME, Constants.TARGET_TAG_TABLE_NAME, spark)
      val clickDeviceDfrom = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_DFROM_TABLE_NAME, Constants.TARGET_DFROM_TABLE_NAME, spark)
      val clickDeviceCategoryDfrom = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_CATEGORY_DFROM_TABLE_NAME, Constants.TARGET_CATEGORY_DFROM_TABLE_NAME, spark)
      val clickDeviceCategoryTag = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_CATEGORY_TAG_TABLE_NAME, Constants.TARGET_CATEGORY_TAG_TABLE_NAME, spark)
      //      val clickDeviceT64 = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_T64_TABLE_NAME, Constants.TARGET_T64_TABLE_NAME, spark)
      //      val clickDeviceT128 = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_T128_TABLE_NAME, Constants.TARGET_T128_TABLE_NAME, spark)
      //      val clickDeviceT256 = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_T256_TABLE_NAME, Constants.TARGET_T256_TABLE_NAME, spark)
      //      val clickDeviceT512 = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_T512_TABLE_NAME, Constants.TARGET_T512_TABLE_NAME, spark)
      //      val clickDeviceT1000 = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_CLICK_TABLE_NAME, Constants.NEWS_T1000_TABLE_NAME, Constants.TARGET_T1000_TABLE_NAME, spark)

      val inviewDeviceCategory = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_CATEGORY_TABLE_NAME, Constants.TARGET_CATEGORY_TABLE_NAME, spark)
      val inviewDeviceSecondCate = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_SECOND_CATE_TABLE_NAME, Constants.TARGET_SECOND_CATE_TABLE_NAME, spark)
      val inviewDeviceTag = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_TAG_TABLE_NAME, Constants.TARGET_TAG_TABLE_NAME, spark)
      val inviewDeviceDfrom = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_DFROM_TABLE_NAME, Constants.TARGET_DFROM_TABLE_NAME, spark)
      val inviewDeviceCategoryDfrom = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_CATEGORY_DFROM_TABLE_NAME, Constants.TARGET_CATEGORY_DFROM_TABLE_NAME, spark)
      val inviewDeviceCategoryTag = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_CATEGORY_TAG_TABLE_NAME, Constants.TARGET_CATEGORY_TAG_TABLE_NAME, spark)
      //      val invieweDviceT64 = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_T64_TABLE_NAME, Constants.TARGET_T64_TABLE_NAME, spark)
      //      val invieweDviceT128 = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_T128_TABLE_NAME, Constants.TARGET_T128_TABLE_NAME, spark)
      //      val invieweDviceT256 = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_T256_TABLE_NAME, Constants.TARGET_T256_TABLE_NAME, spark)
      //      val invieweDviceT512 = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_T512_TABLE_NAME, Constants.TARGET_T512_TABLE_NAME, spark)
      //      val invieweDviceT1000 = CalculateUtils.getTargetJoinDataframe(Constants.BASIC_INVIEW_TABLE_NAME, Constants.NEWS_T1000_TABLE_NAME, Constants.TARGET_T1000_TABLE_NAME, spark)
      //      println("clickDeviceCategory数量为" + clickDeviceCategory.count)
      //      println("clickDeviceSecondCate数量为" + clickDeviceSecondCate.count)
      //      println("clickDeviceTag数量为" + clickDeviceTag.count)
      //      println("clickDeviceDfrom数量为" + clickDeviceDfrom.count)
      //      println("clickDeviceCategoryDfrom数量为" + clickDeviceCategoryDfrom.count)
      //      println("clickDeviceCategoryTag数量为" + clickDeviceCategoryTag.count)
      //      println("inviewDeviceCategory数量为" + inviewDeviceCategory.count)
      //      println("inviewDeviceSecondCate数量为" + inviewDeviceSecondCate.count)
      //      println("inviewDeviceTag数量为" + inviewDeviceTag.count)
      //      println("inviewDeviceDfrom数量为" + inviewDeviceDfrom.count)
      //      println("inviewDeviceCategoryDfrom数量为" + inviewDeviceCategoryDfrom.count)
      //      println("inviewDeviceCategoryTag数量为" + inviewDeviceCategoryTag.count)


      clickDeviceCategory.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_CATEGORY_TABLE_NAME)
      clickDeviceSecondCate.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_SECOND_CATE_TABLE_NAME)
      clickDeviceTag.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_TAG_TABLE_NAME)
      clickDeviceDfrom.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_DFROM_TABLE_NAME)
      clickDeviceCategoryDfrom.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_CATEGORY_DFROM_TABLE_NAME)
      clickDeviceCategoryTag.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_CATEGORY_TAG_TABLE_NAME)
      //      clickDeviceT64.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_T64_TABLE_NAME)
      //      clickDeviceT128.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_T128_TABLE_NAME)
      //      clickDeviceT256.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_T256_TABLE_NAME)
      //      clickDeviceT512.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_T512_TABLE_NAME)
      //      clickDeviceT1000.createOrReplaceTempView(Constants.JOIN_CLICK_DEVICE_T1000_TABLE_NAME)
      inviewDeviceCategory.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_CATEGORY_TABLE_NAME)
      inviewDeviceSecondCate.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_SECOND_CATE_TABLE_NAME)
      inviewDeviceTag.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_TAG_TABLE_NAME)
      inviewDeviceDfrom.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_DFROM_TABLE_NAME)
      inviewDeviceCategoryDfrom.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_CATEGORY_DFROM_TABLE_NAME)
      inviewDeviceCategoryTag.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_CATEGORY_TAG_TABLE_NAME)
      //      invieweDviceT64.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_T64_TABLE_NAME)
      //      invieweDviceT128.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_T128_TABLE_NAME)
      //      invieweDviceT256.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_T256_TABLE_NAME)
      //      invieweDviceT512.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_T512_TABLE_NAME)
      //      invieweDviceT1000.createOrReplaceTempView(Constants.JOIN_INVIEW_DEVICE_T1000_TABLE_NAME)

      val categoryResultRdd = CalculateUtils.getTargetResult(Constants.JOIN_CLICK_DEVICE_CATEGORY_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_CATEGORY_TABLE_NAME, Constants.TARGET_CATEGORY_TABLE_NAME, Constants.TARGET_CATEGORY_JSON_NAME, spark, yesterday)
      val secondCateResultRdd = CalculateUtils.getTargetResult(Constants.JOIN_CLICK_DEVICE_SECOND_CATE_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_SECOND_CATE_TABLE_NAME, Constants.TARGET_SECOND_CATE_TABLE_NAME, Constants.TARGET_SECOND_CATE_JSON_NAME, spark, yesterday)
      val tagResultRdd = CalculateUtils.getTargetResult(Constants.JOIN_CLICK_DEVICE_TAG_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_TAG_TABLE_NAME, Constants.TARGET_TAG_TABLE_NAME, Constants.TARGET_TAG_JSON_NAME, spark, yesterday)
      val dfromResultRdd = CalculateUtils.getTargetResult(Constants.JOIN_CLICK_DEVICE_DFROM_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_DFROM_TABLE_NAME, Constants.TARGET_DFROM_TABLE_NAME, Constants.TARGET_DFROM_JSON_NAME, spark, yesterday)
      val categoryDfromResultRdd = CalculateUtils.getTargetResult(Constants.JOIN_CLICK_DEVICE_CATEGORY_DFROM_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_CATEGORY_DFROM_TABLE_NAME, Constants.TARGET_CATEGORY_DFROM_TABLE_NAME, Constants.TARGET_CATEGORY_DFROM_JSON_NAME, spark, yesterday)
      val categoryTagResultRdd = CalculateUtils.getTargetResult(Constants.JOIN_CLICK_DEVICE_CATEGORY_TAG_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_CATEGORY_TAG_TABLE_NAME, Constants.TARGET_CATEGORY_TAG_TABLE_NAME, Constants.TARGET_CATEGORY_TAG_JSON_NAME, spark, yesterday)
      //      val t64ResultRdd = CalculateUtils.getTargetResult(Constants.JOIN_CLICK_DEVICE_T64_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_T64_TABLE_NAME, Constants.TARGET_T64_TABLE_NAME, Constants.TARGET_T64_JSON_NAME, spark)
      //      val t128ResultRdd = CalculateUtils.getTargetResult(Constants.JOIN_CLICK_DEVICE_T128_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_T128_TABLE_NAME, Constants.TARGET_T128_TABLE_NAME, Constants.TARGET_T128_JSON_NAME, spark)
      //      val t256ResultRdd = CalculateUtils.getTargetResult(Constants.JOIN_CLICK_DEVICE_T256_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_T256_TABLE_NAME, Constants.TARGET_T256_TABLE_NAME, Constants.TARGET_T256_JSON_NAME, spark)
      //      val t512ResultRdd = CalculateUtils.getTargetResult(Constants.JOIN_CLICK_DEVICE_T512_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_T512_TABLE_NAME, Constants.TARGET_T512_TABLE_NAME, Constants.TARGET_T512_JSON_NAME, spark)
      //      val t1000ResultRdd = CalculateUtils.getTargetResult(Constants.JOIN_CLICK_DEVICE_T1000_TABLE_NAME, Constants.JOIN_INVIEW_DEVICE_T1000_TABLE_NAME, Constants.TARGET_T1000_TABLE_NAME, Constants.TARGET_T1000_JSON_NAME, spark)
      //      println("categoryResultRdd数量为" + categoryResultRdd.count)
      //      println("secondCateResultRdd数量为" + secondCateResultRdd.count)
      //      println("tagResultRdd数量为" + tagResultRdd.count)
      //      println("dfromResultRdd数量为" + dfromResultRdd.count)
      //      println("categoryDfromResultRdd数量为" + categoryDfromResultRdd.count)
      //      println("categoryTagResultRdd数量为" + categoryTagResultRdd.count)


      val unionResultRdd = categoryResultRdd
        .union(secondCateResultRdd)
        .union(tagResultRdd)
        .union(dfromResultRdd)
        .union(categoryDfromResultRdd)
        .union(categoryTagResultRdd)
      //        .union(t64ResultRdd)
      //        .union(t128ResultRdd)
      //        .union(t256ResultRdd)
      //        .union(t512ResultRdd)
      //        .union(t1000ResultRdd)
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

      finalRdd.repartition(20).saveAsTextFile(hdfs_path)
      //      println(finalRdd.first)
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
