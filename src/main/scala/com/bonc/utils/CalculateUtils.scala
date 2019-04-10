package com.bonc.utils

import com.alibaba.fastjson.JSONObject
import com.bonc.constants.Constants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * Author: YangYunhe
  * Description: 计算用到的工具类
  * Create: 2018-07-03 16:10
  */
object CalculateUtils {

  /**
    * 过滤空值，包括：
    * "category": []
    * "secondCate": []
    * "tag": []
    * "t1000": []
    * "t64": []
    * "t128": []
    * "t256": []
    * "t512": []
    */
  //  def filterNewsRdd(newsRdd: RDD[JSONObject]): RDD[JSONObject] = {
  //    newsRdd.filter(json => {
  //      json.get(Constants.TARGET_CATEGORY_JSON_NAME).toString.length > 2 &&
  //        json.get(Constants.TARGET_SECOND_CATE_JSON_NAME).toString.length > 2 &&
  //        json.get(Constants.TARGET_TAG_JSON_NAME).toString.length > 2 &&
  //        json.get(Constants.TARGET_T1000_JSON_NAME).toString.length > 2 &&
  //        json.get(Constants.TARGET_T64_JSON_NAME).toString.length > 2 &&
  //        json.get(Constants.TARGET_T128_JSON_NAME).toString.length > 2 &&
  //        json.get(Constants.TARGET_T256_JSON_NAME).toString.length > 2 &&
  //        json.get(Constants.TARGET_T512_JSON_NAME).toString.length > 2
  //    })
  //  }

  /**
    * 提取 newsRdd 中需要使用的指标
    */
  def getTargetNewsRdd(newsFilterJSONObjectRdd: RDD[JSONObject], target: String): RDD[String] = {
    newsFilterJSONObjectRdd.
      filter(x => {
        x.containsKey(target) &&
          x.get(target) != null &&
          x.get(target).toString.length > 2
      })
      .map(x => {
        val newsId = x.get(Constants.TARGET_ID_JSON_NAME).toString
        val targetJsonStr = x.get(target).toString // ["体育","情感"]
        val targetsStr = targetJsonStr.substring(1, targetJsonStr.length - 1) // "体育","情感"
        val targets = targetsStr.split(",")
        var result = ""
        for (x <- targets) {
          result += newsId + ":" + x.trim + ","
        }
        result.dropRight(1)
      })
      .distinct()
      .flatMap(_.split(","))
  }

  /**
    * newsRdd 中的指标from
    */
  def getDFromRdd(newsFilterJSONObjectRdd: RDD[JSONObject], target: String): RDD[String] = {
    newsFilterJSONObjectRdd.
      filter(x => {
        //newsprofile为做特殊处理需要排除异常数据
        x.get(target) != null &&
          x.containsKey(Constants.TARGET_DFROM_JSON_NAME)
      })
      .map(x => {
        val newsId = Kutil.getString(x, Constants.TARGET_ID_JSON_NAME, "null")
        val targetJsonStr = x.get(target).toString // "from":"创意生活DIY"
        newsId + ":" + targetJsonStr.trim
      })
      .distinct()
  }

  /**
    * 提取 newsRdd 中交叉指标category_from
    */
  def getCategoryDfromNewsRdd(newsFilterJSONObjectRdd: RDD[JSONObject]): RDD[String] = {
    newsFilterJSONObjectRdd
      .filter(x => {
        x.get(Constants.TARGET_CATEGORY_JSON_NAME).toString.length > 2 &&
          x.get(Constants.TARGET_DFROM_JSON_NAME) != "null" &&
          x.containsKey(Constants.TARGET_DFROM_JSON_NAME) &&
          x.containsKey(Constants.TARGET_CATEGORY_JSON_NAME)
      })
      .map(x => {
        val newsId = x.get(Constants.TARGET_ID_JSON_NAME).toString
        val categoryJsonStr = x.get(Constants.TARGET_CATEGORY_JSON_NAME).toString
        val categoryStr = categoryJsonStr.substring(1, categoryJsonStr.length - 1)
        val categorys = categoryStr.split(",")
        val dfromJsonStr = x.get(Constants.TARGET_DFROM_JSON_NAME).toString
        val categorys_from = categorys.map(x => {
          x + "-" + dfromJsonStr
        })
        var result = ""
        for (x <- categorys_from) {
          result += newsId + ":" + x.trim + ","
        }
        result.dropRight(1)
      })
      .distinct()
      //20180927的新闻，4852787517860034989	["文化","职场"]	岁月如歌,孤影阑珊，dfrom中带逗号会造成flatMap异常
      //4852787517860034989：文化-岁月如歌，孤影阑珊，4852787517860034989：...   最终孤影阑珊单独flatMap出来造成193行：分割数组下标越界
      .flatMap(_.split(","))
  }

  /**
    * 提取 newsRDD 中交叉指标category_tag,每天1/5的新闻没有tag,不用做训练数据
    */
  def getCategoryTagNewsRdd(newsFilterJSONObjectRdd: RDD[JSONObject]): RDD[String] = {
    newsFilterJSONObjectRdd
      .filter(x => {
        x.get(Constants.TARGET_CATEGORY_JSON_NAME).toString.length > 2 &&
          x.get(Constants.TARGET_TAG_JSON_NAME).toString.length > 2 &&
          x.containsKey(Constants.TARGET_TAG_JSON_NAME) &&
          x.containsKey(Constants.TARGET_CATEGORY_JSON_NAME)
      })

      .map(x => {
        val newsId = x.get(Constants.TARGET_ID_JSON_NAME).toString
        val categoryJsonStr = x.get(Constants.TARGET_CATEGORY_JSON_NAME).toString // ["体育","情感"]
        val categoryStr = categoryJsonStr.substring(1, categoryJsonStr.length - 1) // "体育","情感"
        val categorys = categoryStr.split(",")
        val tagJsonStr = x.get(Constants.TARGET_TAG_JSON_NAME).toString
        val tagStr = tagJsonStr.substring(1, tagJsonStr.length - 1) // "体育","情感"
        val tags = tagStr.split(",")
        val category_tags = categorys.flatMap(x => {
          tags.map(y => {
            x + "-" + y
          })
        })
        var result = ""
        for (x <- category_tags) {
          result += newsId + ":" + x.trim + ","
        }
        result.dropRight(1)
      })
      .distinct()
      .flatMap(_.split(","))
  }

  /**
    * 处理 click 和 inview 两个原始 json 字符串 rdd，使其的数据格式变为 "newsId:deviceId"
    * 增加时间衰减系数，每天0.99
    */
  def getNewsDeviceRdd(jsonObjectRdd: RDD[JSONObject], spark: SparkSession): RDD[String] = {
    import spark.implicits._
    val df = jsonObjectRdd
      .filter(x => {
        x.get(Constants.TARGET_TS_JSON_NAME).toString != "null"
      })
      .map(json => {
        val deviceId = json.get(Constants.TARGET_DEVICEID_JSON_NAME).toString
        val newsId = json.get(Constants.TARGET_NEWSID_JSON_NAME).toString
        val time = System.currentTimeMillis()
        val time0 = json.get(Constants.TARGET_TS_JSON_NAME).toString.toLong
        //数据时间-当前时间，得到的天数向上取整
        val score = Math.pow(0.99, Math.ceil((time - time0.toLong) / 86400000.0).toInt)
        //        newsId + ":" + deviceId + ":" + score
        (deviceId, newsId, score)
      })
      //根据deivceid,newsid去重，理论上统一用户的同一新闻(上报)只会点击/曝光一次
      .toDF("deviceid", "newsid", "score")
    df.createOrReplaceTempView("temp")
    spark.sql(
      """
        |select deviceid,newsid,score from
        |(select deviceid,newsid,score,row_number() over(partition by deviceid,newsid order by score desc) rn from temp) t
        |where rn=1
      """.stripMargin)
      .rdd
      .map(x => {
        val deviceId = x.get(0).toString
        val newsId = x.get(1).toString
        val score = x.get(2).toString
        newsId + ":" + deviceId + ":" + score
      })
    //      .distinct()
  }

  def registerBasicTable(columnName1: String, columnName2: String, tableName: String, rdd: RDD[String], spark: SparkSession): Unit = {
    val fields = StructField(columnName1, StringType, nullable = false) ::
      StructField(columnName2, StringType, nullable = false) :: Nil
    val schema = StructType(fields)
    val rowRdd = rdd.map(x => {
      val newsId = x.split(":")(0).trim
      try {
        val target = x.split(":")(1).trim
        Row(newsId, target)
      } catch {
        case e => e.printStackTrace()
          println(s".......Exception${columnName2}" + x)
          Row(newsId, s"null_${columnName2}")
      }
    })
    spark.createDataFrame(rowRdd, schema).createOrReplaceTempView(tableName)
    //    println(s"newsprofile拆分：${tableName}")
    //    println(s"数量为：" + rowRdd.count())
    //    spark.sql(s"select * from ${tableName} limit 10").show(10)
  }

  def registerScoreTable(columnName1: String, columnName2: String, columnName3: String, tableName: String, rdd: RDD[String], spark: SparkSession): Unit = {
    val fields = StructField(columnName1, StringType, nullable = false) ::
      StructField(columnName2, StringType, nullable = false) ::
      StructField(columnName3, DoubleType, nullable = false) :: Nil
    val schema = StructType(fields)
    val rowRdd = rdd.map(x => {
      val newsId = x.split(":")(0).trim
      val deviceid = x.split(":")(1).trim
      val score = x.split(":")(2).trim.toDouble
      Row(newsId, deviceid, score)
    })
    spark.createDataFrame(rowRdd, schema).createOrReplaceTempView(tableName)
    //    println(s"用户点击/曝光新闻含衰减评分：${tableName}")
    //    println(s"数量为：" + rowRdd.count())
    //    spark.sql(s"select * from ${tableName} limit 10").show(10)
  }

  def getTargetJoinDataframe(tableName: String, newsTableName: String, target: String, spark: SparkSession): DataFrame = {
    val sql =
      s"""
         |SELECT ${tableName}.${Constants.TARGET_DEVICEID_TABLE_NAME},
         |${tableName}.${Constants.TARGET_SCORE_TABLE_NAME},
         |${newsTableName}.${target}
         |FROM ${tableName}, ${newsTableName}
         |WHERE ${tableName}.${Constants.TARGET_NEWSID_TABLE_NAME} = ${newsTableName}.${Constants.TARGET_NEWSID_TABLE_NAME}
      """.stripMargin
    val df = spark.sql(sql)
      .cache()
    //    println(s"用户点击/曝光feature含衰减评分：${tableName}")
    //    println(s"数量为：" + df.count())
    //    df.show(10)
    df
    //    spark.sql(sql)
  }

  /*用户获取常规的新闻用户画像*/
  def getTargetResult(joinClickTableName: String, joinInviewTable: String, target: String, targetJsonName: String, spark: SparkSession, yesterday: String): RDD[String] = {

    /**
      * 每个用户点击各个一级品类的次数
      * 866401030845468, "育儿", 7
      * bd7cf281302717fa, "娱乐", 25
      */
    spark.udf.register("wilson", wilson)

    val userClickTargetCount =
      spark.sql(
        s"""
           |SELECT ${Constants.TARGET_DEVICEID_TABLE_NAME}, ${target}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS user_click_${target}_count
           |FROM ${joinClickTableName}
           |GROUP BY ${Constants.TARGET_DEVICEID_TABLE_NAME}, ${target}
        """.stripMargin)
    userClickTargetCount.cache()
    userClickTargetCount.createOrReplaceTempView(s"user_click_${target}_count_table")
    //    println(s"用户点击各个一级feature：${target}")
    //    println(s"数量为：" + userClickTargetCount.count())
    //    spark.sql(s"select * from user_click_${target}_count_table limit 10").show(10)
    /**
      * 每个用户点击所有一级品类的次数
      * c864071033940939, 282
      * ae46bca04c71cfb8, 5
      */
    val userClickTargetsCount =
      spark.sql(
        s"""
           |SELECT ${Constants.TARGET_DEVICEID_TABLE_NAME}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS user_click_${target}s_count
           |FROM ${joinClickTableName}
           |GROUP BY ${Constants.TARGET_DEVICEID_TABLE_NAME}
        """.stripMargin)
    userClickTargetsCount .cache()
    userClickTargetsCount.createTempView(s"user_click_${target}s_count_table")
    //    println(s"用户点击所有一级类别：${target}")
    //    println(s"数量为：" + userClickTargetsCount.count())
    //    spark.sql(s"select * from user_click_${target}s_count_table limit 10").show(10)

    /**
      * 每个用户看到各个一级品类的次数
      */
    val userInviewTargetCount =
      spark.sql(
        s"""
           |SELECT ${Constants.TARGET_DEVICEID_TABLE_NAME}, ${target}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS user_inview_${target}_count
           |FROM ${joinInviewTable}
           |GROUP BY ${Constants.TARGET_DEVICEID_TABLE_NAME}, ${target}
        """.stripMargin)
    userInviewTargetCount .cache()
    userInviewTargetCount.createOrReplaceTempView(s"user_inview_${target}_count_table")
    //    println(s"用户曝光各个一级feature：${target}")
    //    println(s"数量为：" + userInviewTargetCount.count())
    //    spark.sql(s"select * from user_inview_${target}_count_table limit 10").show(10)

    /**
      * 每个一级类别被点击的总次数
      * "科学", 1628
      * "财经", 1604
      */
    val targetClickCount =
      spark.sql(
        s"""
           |SELECT ${target}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS ${target}_click_count
           |FROM ${joinClickTableName}
           |group by ${target}
        """.stripMargin)
    targetClickCount .cache()
    targetClickCount.createOrReplaceTempView(s"${target}_click_count_table")
    //    println(s"每个一级类别被点击总次数：${target}")
    //    println(s"数量为：" + targetClickCount.count())
    //    spark.sql(s"select * from ${target}_click_count_table limit 10").show(10)

    /**
      * 每个一级类别被看到的次数
      */
    val targetInviewCount =
      spark.sql(
        s"""
           |SELECT ${target}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS ${target}_inview_count
           |FROM ${joinInviewTable}
           |group by ${target}
        """.stripMargin)
    targetInviewCount.cache()
    targetInviewCount.createOrReplaceTempView(s"${target}_inview_count_table")
    //    println(s"每个一级类别被看到的次数：${target}")
    //    println(s"数量为：" + targetInviewCount.count())
    //    spark.sql(s"select * from ${target}_inview_count_table limit 10").show(10)

    /**
      * 各类用户点击所有的一级品类的次数
      */
    val typeUserClickTargetsCount =
      spark.sql(
        s"""
           |SELECT
           | ccct.${target} AS user_type,
           | sum(ucccts.user_click_${target}s_count) AS type_user_click_${target}s_counts
           |FROM
           | ${target}_click_count_table ccct,
           | user_click_${target}s_count_table ucccts,
           | user_click_${target}_count_table uccct
           |WHERE ccct.${target} = uccct.${target}
           |AND uccct.${Constants.TARGET_DEVICEID_TABLE_NAME} = ucccts.${Constants.TARGET_DEVICEID_TABLE_NAME}
           |GROUP BY ccct.${target}
        """.stripMargin)
    typeUserClickTargetsCount.cache()
    typeUserClickTargetsCount.createOrReplaceTempView(s"type_user_click_${target}s_count_table")
    //    println(s"类别用户点击所有一级品类次数：${target}")
    //    println(s"数量为：" + typeUserClickTargetsCount.count())
    //    spark.sql(s"select * from type_user_click_${target}s_count_table limit 10").show(10)

    //

    /**
      * 计算 CS
      */
    val userTargetCS =
      spark.sql(
        s"""
           |SELECT
           | uccct.${Constants.TARGET_DEVICEID_TABLE_NAME},
           | uccct.${target},
           | (wilson(ucccts.user_click_${target}s_count,uccct.user_click_${target}_count,1) / wilson(tuccct.type_user_click_${target}s_counts,ccct.${target}_click_count,1)) AS CS
           |FROM
           |  user_click_${target}_count_table uccct,
           |  user_click_${target}s_count_table ucccts,
           |  ${target}_click_count_table ccct,
           |  type_user_click_${target}s_count_table tuccct
           |WHERE uccct.${Constants.TARGET_DEVICEID_TABLE_NAME} = ucccts.${Constants.TARGET_DEVICEID_TABLE_NAME}
           |AND uccct.${target} = ccct.${target}
           |AND uccct.${target} = tuccct.user_type
    """.stripMargin)
    userTargetCS.cache()
    //    println(s"cs计算结果：${target}")
    //    println(s"数量为：" + userTargetCS.count())
    //    userTargetCS.show(10)
    //    userTargetCS.createTempView(s"user_${target}_cs")

    /**
      * 计算 cs_smooth
      */
    //    val userTargetCsSmooth =
    //      spark.sql(
    //        s"""
    //           |SELECT
    //           | uccct.${Constants.TARGET_DEVICEID_TABLE_NAME},
    //           | uccct.${target},
    //           | (((uccct.user_click_${target}_count + 3) / (ucccts.user_click_${target}s_count + (3 * (tuccct.type_user_click_${target}s_counts / ccct.${target}_click_count)))) / (ccct.${target}_click_count / tuccct.type_user_click_${target}s_counts)) AS CS_SMOOTH
    //           |FROM
    //           |  user_click_${target}_count_table uccct,
    //           |  user_click_${target}s_count_table ucccts,
    //           |  ${target}_click_count_table ccct,
    //           |  type_user_click_${target}s_count_table tuccct
    //           |WHERE uccct.${Constants.TARGET_DEVICEID_TABLE_NAME} = ucccts.${Constants.TARGET_DEVICEID_TABLE_NAME}
    //           |AND uccct.${target} = ccct.${target}
    //           |AND uccct.${target} = tuccct.user_type
    //    """.stripMargin)

    //    userTargetCsSmooth.createTempView(s"user_${target}_cs_smooth")

    /**
      * 计算 CK
      */
    val userTargetCK =
      spark.sql(
        s"""
           |SELECT
           | uccct.${Constants.TARGET_DEVICEID_TABLE_NAME},
           | uccct.${target},
           | (wilson(uicct.user_inview_${target}_count,uccct.user_click_${target}_count,2) / wilson(cict.${target}_inview_count,ccct.${target}_click_count,2)) AS CK
           |FROM
           |  user_click_${target}_count_table uccct,
           |  user_inview_${target}_count_table uicct,
           |  ${target}_click_count_table ccct,
           |  ${target}_inview_count_table cict
           |WHERE uccct.${Constants.TARGET_DEVICEID_TABLE_NAME} = uicct.${Constants.TARGET_DEVICEID_TABLE_NAME}
           |AND uccct.${target} = uicct.${target}
           |AND uccct.${target} = ccct.${target}
           |AND uccct.${target} = cict.${target}
            """.stripMargin)
    userTargetCK.cache()
    /**
      * 计算 CK，分母使用威尔逊，分子直接使用比值，但是要关联出所有inview的数据
      */
    //    val sql =
    //      s"""
    //         |select
    //         |${Constants.TARGET_DEVICEID_TABLE_NAME},
    //         |${target},
    //         |case when CK is null then 0 else CK end as CK
    //         | from
    //         |(select ${Constants.TARGET_DEVICEID_TABLE_NAME},
    //         |${target},
    //         |(((user_click_${target}_count+0.1)/user_inview_${target}_count) / wilson(${target}_inview_count,${target}_click_count,2)) AS CK
    //         | from
    //         |(select
    //         |uicct.${Constants.TARGET_DEVICEID_TABLE_NAME},
    //         |uicct.${target},
    //         |uicct.user_inview_${target}_count,
    //         |case when uccct.user_click_${target}_count is null then 0 else uccct.user_click_${target}_count end as user_click_${target}_count,
    //         |case when cict.${target}_inview_count is null then 0 else cict.${target}_inview_count end as ${target}_inview_count,
    //         |case when ccct.${target}_click_count is null then 0 else ccct.${target}_click_count end as ${target}_click_count
    //         | from
    //         |user_inview_${target}_count_table uicct
    //         |left join
    //         |user_click_${target}_count_table uccct
    //         |on uicct.${target}=uccct.${target} and uicct.${Constants.TARGET_DEVICEID_TABLE_NAME}=uccct.${Constants.TARGET_DEVICEID_TABLE_NAME}
    //         |left join
    //         |${target}_inview_count_table cict
    //         |on uicct.${target}=cict.${target}
    //         |left join
    //         |${target}_click_count_table ccct
    //         |on uicct.${target}=ccct.${target}
    //         |) t
    //         |) t
    //        """.stripMargin
    //    println(sql)
    //    val userTargetCK = spark.sql(sql)

    //ck计算使用前一天的校验值，注意关联时去除引号
    //    val sql =
    //      s"""
    //         |select
    //         |${Constants.TARGET_DEVICEID_TABLE_NAME},
    //         |${target},
    //         |case when CK is null then 0 else CK end as CK
    //         | from
    //         |(select ${Constants.TARGET_DEVICEID_TABLE_NAME},
    //         |${target},
    //         |(wilson(user_inview_${target}_count,(user_click_${target}_count+avg),2) / wilson(${target}_inview_count,${target}_click_count,2)) AS CK
    //         | from
    //         |(select
    //         |uicct.${Constants.TARGET_DEVICEID_TABLE_NAME},
    //         |uicct.${target},
    //         |uicct.user_inview_${target}_count,
    //         |case when uccct.user_click_${target}_count is null then 0 else uccct.user_click_${target}_count end as user_click_${target}_count,
    //         |case when cict.${target}_inview_count is null then 0 else cict.${target}_inview_count end as ${target}_inview_count,
    //         |case when ccct.${target}_click_count is null then 0 else ccct.${target}_click_count end as ${target}_click_count,
    //         |upf.avg
    //         | from
    //         |user_inview_${target}_count_table uicct
    //         |left join
    //         |user_click_${target}_count_table uccct
    //         |on uicct.${target}=uccct.${target} and uicct.${Constants.TARGET_DEVICEID_TABLE_NAME}=uccct.${Constants.TARGET_DEVICEID_TABLE_NAME}
    //         |left join
    //         |${target}_inview_count_table cict
    //         |on uicct.${target}=cict.${target}
    //         |left join
    //         |${target}_click_count_table ccct
    //         |on uicct.${target}=ccct.${target}
    //         |left join
    //         |(
    //         |select split(target,'_')[1] as target,avg from userprofile.user_profile_feature_stat
    //         |where pday=${yesterday}
    //         |and split(target,'_')[0]='${targetJsonName}'
    //         |and split(target,'_')[2]='ck'
    //         |) upf
    //         |on replace(uicct.${target},'\"','')=upf.target
    //         |) t
    //         |) t
    //    """.stripMargin
    //    println(sql)
    //    val userTargetCK = spark.sql(sql)
    //    userTargetCK.cache()
    //    println(s"ck计算结果：${target}")
    //    println(s"数量为：" + userTargetCK.count())
    //    userTargetCK.show(10)
    //    userTargetCK.createTempView(s"user_${target}_ck")

    //如果是tag取前500
    if (targetJsonName == "tag") {
      val userTargetCSRdd = userTargetCS.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          .replaceAll(""""""", "")
        val csKeyStr = s"${targetJsonName}_${targetTrim}_cs"
        val csValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + csKeyStr + ":" + csValueStr
        val result = deviceId + "||" + csKeyStr + ":" + csValueStr
        (deviceId, csValueStr, result)
      })
      import spark.implicits._
      val tag_df_cs = userTargetCSRdd.toDF("key", "value", "result")
      tag_df_cs.createOrReplaceTempView("tag_order_cs")
      val result_cs = spark.sql(
        """
          |select key,result from
          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_cs
          |) t0
          |where rn<=500
        """.stripMargin)
        .rdd
        .map(_.get(1).toString)

      //对于tag cs大于500的部分作为dislikeTag
      //      val dislikeTag_cs = spark.sql(
      //        """
      //          |select key,result from
      //          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_cs
      //          |) t0
      //          |where rn>500
      //        """.stripMargin)
      //        .rdd
      //        .map{x=>{
      //          val str=x.get(1).toString
      //          val deviceId = str.split("\\|\\|")(0).trim
      //          val feature = str.split("\\|\\|")(1).trim
      //          val dislike_feature="dislikeT"+feature.substring(1)
      //          val result = deviceId + "||" + dislike_feature
      //          result
      //        }}


      val userTargetCKRdd = userTargetCK.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          //          .substring(1, target.length - 1)  from不适用
          .replaceAll(""""""", "")
        val ckKeyStr = s"${targetJsonName}_${targetTrim}_ck"
        val ckValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + ckKeyStr + ":" + ckValueStr
        val result = deviceId + "||" + ckKeyStr + ":" + ckValueStr
        (deviceId, ckValueStr, result)
      })
      val tag_df_ck = userTargetCKRdd.toDF("key", "value", "result")
      tag_df_ck.createOrReplaceTempView("tag_order_ck")
      val result_ck = spark.sql(
        """
          |select key,result from
          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_ck
          |) t0
          |where rn<=500
        """.stripMargin)
        .rdd
        .map(_.get(1).toString)



      //对于tag cs大于500的部分作为dislikeTag
      //      val dislikeTag_ck = spark.sql(
      //        """
      //          |select key,result from
      //          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_ck
      //          |) t0
      //          |where rn>500
      //        """.stripMargin)
      //        .rdd
      //        .map(x=>{
      //          val str=x.get(1).toString
      //          val deviceId = str.split("\\|\\|")(0).trim
      //          val feature = str.split("\\|\\|")(1).trim
      //          val dislike_feature="dislikeT"+feature.substring(1)
      //          val result = deviceId + "||" + dislike_feature
      //          result
      //        })
      //
      //
      //      //      val userTargetCSSmoothRdd = userTargetCsSmooth.rdd.map(row => {
      //      //        val deviceId = row.get(0).toString
      //      //        val target = row.get(1).toString
      //      //        val targetTrim = target.substring(1, target.length - 1)
      //      //        val group_key = s"${targetJsonName}_${targetTrim}"
      //      //        val csSmoothKeyStr = s"${targetJsonName}_${targetTrim}_cs_smooth"
      //      //        val csSmoothValueStr = row.get(2).toString
      //      //        //      deviceId + "||" + group_key + "," + csSmoothKeyStr + ":" + csSmoothValueStr
      //      //        val result = deviceId + "||" + csSmoothKeyStr + ":" + csSmoothValueStr
      //      //        (deviceId, csSmoothValueStr, result)
      //      //      })
      //      //      val tag_df_csSmooth = userTargetCSSmoothRdd.toDF("key", "value", "result")
      //      //      tag_df_csSmooth.createOrReplaceTempView("tag_order_csSmooth")
      //      //      val result_csSmooth = spark.sql(
      //      //        """
      //      //          |select key,result from
      //      //          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_csSmooth
      //      //          |) t0
      //      //          |where rn<=300
      //      //        """.stripMargin)
      //      //        .rdd
      //      //        .map(_.get(1).toString)
      //
      userClickTargetCount.unpersist()
      userClickTargetsCount.unpersist()
      userInviewTargetCount.unpersist()
      targetClickCount.unpersist()
      targetInviewCount.unpersist()
      typeUserClickTargetsCount.unpersist()
      userTargetCS.unpersist()
      userTargetCK.unpersist()
      result_cs.union(result_ck)
      //        .union(dislikeTag_cs).union(dislikeTag_ck)
      //      //        .union(result_csSmooth)
    }
    //from取前100
    else if (targetJsonName == "from") {
      val userTargetCSRdd = userTargetCS.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          .replaceAll(""""""", "")
        val csKeyStr = s"${targetJsonName}_${targetTrim}_cs"
        val csValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + csKeyStr + ":" + csValueStr
        val result = deviceId + "||" + csKeyStr + ":" + csValueStr
        (deviceId, csValueStr, result)
      })
      import spark.implicits._
      val tag_df_cs = userTargetCSRdd.toDF("key", "value", "result")
      tag_df_cs.createOrReplaceTempView("tag_order_cs")
      val result_cs = spark.sql(
        """
          |select key,result from
          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_cs
          |) t0
          |where rn<=100
        """.stripMargin)
        .rdd
        .map(_.get(1).toString)
      val userTargetCKRdd = userTargetCK.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          //          .substring(1, target.length - 1)  from不适用
          .replaceAll(""""""", "")
        val ckKeyStr = s"${targetJsonName}_${targetTrim}_ck"
        val ckValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + ckKeyStr + ":" + ckValueStr
        val result = deviceId + "||" + ckKeyStr + ":" + ckValueStr
        (deviceId, ckValueStr, result)
      })
      val tag_df_ck = userTargetCKRdd.toDF("key", "value", "result")
      tag_df_ck.createOrReplaceTempView("tag_order_ck")
      val result_ck = spark.sql(
        """
          |select key,result from
          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_ck
          |) t0
          |where rn<=100
        """.stripMargin)
        .rdd
        .map(_.get(1).toString)
      userClickTargetCount.unpersist()
      userClickTargetsCount.unpersist()
      userInviewTargetCount.unpersist()
      targetClickCount.unpersist()
      targetInviewCount.unpersist()
      typeUserClickTargetsCount.unpersist()
      userTargetCS.unpersist()
      userTargetCK.unpersist()
      result_cs.union(result_ck)
    }
    //categoryDfrom取前200
    else if (targetJsonName == "categoryDfrom") {
      val userTargetCSRdd = userTargetCS.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          .replaceAll(""""""", "")
        val csKeyStr = s"${targetJsonName}_${targetTrim}_cs"
        val csValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + csKeyStr + ":" + csValueStr
        val result = deviceId + "||" + csKeyStr + ":" + csValueStr
        (deviceId, csValueStr, result)
      })
      import spark.implicits._
      val tag_df_cs = userTargetCSRdd.toDF("key", "value", "result")
      tag_df_cs.createOrReplaceTempView("tag_order_cs")
      val result_cs = spark.sql(
        """
          |select key,result from
          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_cs
          |) t0
          |where rn<=200
        """.stripMargin)
        .rdd
        .map(_.get(1).toString)
      val userTargetCKRdd = userTargetCK.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          //          .substring(1, target.length - 1)  from不适用
          .replaceAll(""""""", "")
        val ckKeyStr = s"${targetJsonName}_${targetTrim}_ck"
        val ckValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + ckKeyStr + ":" + ckValueStr
        val result = deviceId + "||" + ckKeyStr + ":" + ckValueStr
        (deviceId, ckValueStr, result)
      })
      val tag_df_ck = userTargetCKRdd.toDF("key", "value", "result")
      tag_df_ck.createOrReplaceTempView("tag_order_ck")
      val result_ck = spark.sql(
        """
          |select key,result from
          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_ck
          |) t0
          |where rn<=200
        """.stripMargin)
        .rdd
        .map(_.get(1).toString)
      userClickTargetCount.unpersist()
      userClickTargetsCount.unpersist()
      userInviewTargetCount.unpersist()
      targetClickCount.unpersist()
      targetInviewCount.unpersist()
      typeUserClickTargetsCount.unpersist()
      userTargetCS.unpersist()
      userTargetCK.unpersist()
      result_cs.union(result_ck)
    }
    //t1000排序取前500
    //    else if (targetJsonName == "t1000") {
    //      val userTargetCSRdd = userTargetCS.rdd.map(row => {
    //        val deviceId = row.get(0).toString
    //        val target = row.get(1).toString
    //        val targetTrim = target.substring(1, target.length - 1)
    //        val group_key = s"${targetJsonName}_${targetTrim}"
    //        val csKeyStr = s"${targetJsonName}_${targetTrim}_cs"
    //        val csValueStr = row.get(2).toString
    //        //      deviceId + "||" + group_key + "," + csKeyStr + ":" + csValueStr
    //        val result = deviceId + "||" + csKeyStr + ":" + csValueStr
    //        (deviceId, csValueStr, result)
    //      })
    //      import spark.implicits._
    //      val tag_df_cs = userTargetCSRdd.toDF("key", "value", "result")
    //      tag_df_cs.createOrReplaceTempView("tag_order_cs")
    //      val result_cs = spark.sql(
    //        """
    //          |select key,result from
    //          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_cs
    //          |) t0
    //          |where rn<=500
    //        """.stripMargin)
    //        .rdd
    //        .map(_.get(1).toString)
    //
    //
    //      val userTargetCKRdd = userTargetCK.rdd.map(row => {
    //        val deviceId = row.get(0).toString
    //        val target = row.get(1).toString
    //        val targetTrim = target.substring(1, target.length - 1)
    //        val group_key = s"${targetJsonName}_${targetTrim}"
    //        val ckKeyStr = s"${targetJsonName}_${targetTrim}_ck"
    //        val ckValueStr = row.get(2).toString
    //        //      deviceId + "||" + group_key + "," + ckKeyStr + ":" + ckValueStr
    //        val result = deviceId + "||" + ckKeyStr + ":" + ckValueStr
    //        (deviceId, ckValueStr, result)
    //      })
    //      val tag_df_ck = userTargetCKRdd.toDF("key", "value", "result")
    //      tag_df_ck.createOrReplaceTempView("tag_order_ck")
    //      val result_ck = spark.sql(
    //        """
    //          |select key,result from
    //          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_ck
    //          |) t0
    //          |where rn<=500
    //        """.stripMargin)
    //        .rdd
    //        .map(_.get(1).toString)
    //
    //
    //      val userTargetCSSmoothRdd = userTargetCsSmooth.rdd.map(row => {
    //        val deviceId = row.get(0).toString
    //        val target = row.get(1).toString
    //        val targetTrim = target.substring(1, target.length - 1)
    //        val group_key = s"${targetJsonName}_${targetTrim}"
    //        val csSmoothKeyStr = s"${targetJsonName}_${targetTrim}_cs_smooth"
    //        val csSmoothValueStr = row.get(2).toString
    //        //      deviceId + "||" + group_key + "," + csSmoothKeyStr + ":" + csSmoothValueStr
    //        val result = deviceId + "||" + csSmoothKeyStr + ":" + csSmoothValueStr
    //        (deviceId, csSmoothValueStr, result)
    //      })
    //      val tag_df_csSmooth = userTargetCSSmoothRdd.toDF("key", "value", "result")
    //      tag_df_csSmooth.createOrReplaceTempView("tag_order_csSmooth")
    //      val result_csSmooth = spark.sql(
    //        """
    //          |select key,result from
    //          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_csSmooth
    //          |) t0
    //          |where rn<=500
    //        """.stripMargin)
    //        .rdd
    //        .map(_.get(1).toString)
    //
    //      result_cs.union(result_ck).union(result_csSmooth)
    //        }
    else {
      val userTargetCSRdd = userTargetCS.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          //          .substring(1, target.length - 1)
          .replaceAll(""""""", "")
        //      val group_key = s"${targetJsonName}_${targetTrim}"
        val csKeyStr = s"${targetJsonName}_${targetTrim}_cs"
        val csValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + csKeyStr + ":" + csValueStr
        deviceId + "||" + csKeyStr + ":" + csValueStr
      })


      val userTargetCKRdd: RDD[String] = userTargetCK.rdd.map(row => {
        try {
          val deviceId = row.get(0).toString
          val target = row.get(1).toString
          val targetTrim = target
            //          .substring(1, target.length - 1)  form不适用
            .replaceAll(""""""", "")
          val group_key = s"${targetJsonName}_${targetTrim}"
          val ckKeyStr = s"${targetJsonName}_${targetTrim}_ck"
          val ckValueStr =
            try {
              row.get(2).toString
            }
            catch {
              case e => e.printStackTrace()
                println(row.toString())
                0
            }
          //      deviceId + "||" + group_key + "," + ckKeyStr + ":" + ckValueStr
          deviceId + "||" + ckKeyStr + ":" + ckValueStr
        }
        catch {
          case e => e.printStackTrace()
            println(row.toString())
            "null" + "||" + "null" + 0
        }
      }
      )

      //      val userTargetCSSmoothRdd: RDD[String] = userTargetCsSmooth.rdd.map(row => {
      //        val deviceId = row.get(0).toString
      //        val target = row.get(1).toString
      //        val targetTrim = target.substring(1, target.length - 1)
      //        val group_key = s"${targetJsonName}_${targetTrim}"
      //        val csSmoothKeyStr = s"${targetJsonName}_${targetTrim}_cs_smooth"
      //        val csSmoothValueStr = row.get(2).toString
      //        //      deviceId + "||" + group_key + "," + csSmoothKeyStr + ":" + csSmoothValueStr
      //        deviceId + "||" + csSmoothKeyStr + ":" + csSmoothValueStr
      //      })
      userClickTargetCount.unpersist()
      userClickTargetsCount.unpersist()
      userInviewTargetCount.unpersist()
      targetClickCount.unpersist()
      targetInviewCount.unpersist()
      typeUserClickTargetsCount.unpersist()
      userTargetCS.unpersist()
      userTargetCK.unpersist()
      userTargetCSRdd.union(userTargetCKRdd)
      //        .union(userTargetCSSmoothRdd)
    }
    //    val result =
    //      spark.sql(
    //        s"""
    //          |select
    //          | uccs.${Constants.TARGET_DEVICEID_TABLE_NAME}, uccs.${target}, uccs.CS, ucck.CK, uccss.CS_SMOOTH
    //          |from
    //          | user_${target}_cs uccs,
    //          | user_${target}_ck ucck,
    //          | user_${target}_cs_smooth uccss
    //          |WHERE uccs.${Constants.TARGET_DEVICEID_TABLE_NAME} = ucck.${Constants.TARGET_DEVICEID_TABLE_NAME}
    //          |AND ucck.${Constants.TARGET_DEVICEID_TABLE_NAME} = uccss.${Constants.TARGET_DEVICEID_TABLE_NAME}
    //          |AND uccs.${target} = ucck.${target}
    //          |AND ucck.${target} = uccss.${target}
    //        """.stripMargin).cache()

  }

  /*用于获取CKSmooth的新闻用户画像*/
  def getTargetResultSmooth(joinClickTableName: String, joinInviewTable: String, target: String, targetJsonName: String, spark: SparkSession, yesterday: String): RDD[String] = {

    /**
      * 每个用户点击各个一级品类的次数
      * 866401030845468, "育儿", 7
      * bd7cf281302717fa, "娱乐", 25
      */
    spark.udf.register("wilson", wilson)

    val userClickTargetCount =
      spark.sql(
        s"""
           |SELECT ${Constants.TARGET_DEVICEID_TABLE_NAME}, ${target}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS user_click_${target}_count
           |FROM ${joinClickTableName}
           |GROUP BY ${Constants.TARGET_DEVICEID_TABLE_NAME}, ${target}
        """.stripMargin)
        .cache()
    userClickTargetCount.createOrReplaceTempView(s"user_click_${target}_count_table")
    //    println(s"用户点击各个一级feature：${target}")
    //    println(s"数量为：" + userClickTargetCount.count())
    //    spark.sql(s"select * from user_click_${target}_count_table limit 10").show(10)
    /**
      * 每个用户点击所有一级品类的次数
      * c864071033940939, 282
      * ae46bca04c71cfb8, 5
      */
    val userClickTargetsCount =
      spark.sql(
        s"""
           |SELECT ${Constants.TARGET_DEVICEID_TABLE_NAME}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS user_click_${target}s_count
           |FROM ${joinClickTableName}
           |GROUP BY ${Constants.TARGET_DEVICEID_TABLE_NAME}
        """.stripMargin)
        .cache()
    userClickTargetsCount.createTempView(s"user_click_${target}s_count_table")
    //    println(s"用户点击所有一级类别：${target}")
    //    println(s"数量为：" + userClickTargetsCount.count())
    //    spark.sql(s"select * from user_click_${target}s_count_table limit 10").show(10)

    /**
      * 每个用户看到各个一级品类的次数
      */
    val userInviewTargetCount =
      spark.sql(
        s"""
           |SELECT ${Constants.TARGET_DEVICEID_TABLE_NAME}, ${target}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS user_inview_${target}_count
           |FROM ${joinInviewTable}
           |GROUP BY ${Constants.TARGET_DEVICEID_TABLE_NAME}, ${target}
        """.stripMargin)
        .cache()
    userInviewTargetCount.createOrReplaceTempView(s"user_inview_${target}_count_table")
    //    println(s"用户曝光各个一级feature：${target}")
    //    println(s"数量为：" + userInviewTargetCount.count())
    //    spark.sql(s"select * from user_inview_${target}_count_table limit 10").show(10)

    /**
      * 每个一级类别被点击的总次数
      * "科学", 1628
      * "财经", 1604
      */
    val targetClickCount =
      spark.sql(
        s"""
           |SELECT ${target}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS ${target}_click_count
           |FROM ${joinClickTableName}
           |group by ${target}
        """.stripMargin)
        .cache()
    targetClickCount.createOrReplaceTempView(s"${target}_click_count_table")
    //    println(s"每个一级类别被点击总次数：${target}")
    //    println(s"数量为：" + targetClickCount.count())
    //    spark.sql(s"select * from ${target}_click_count_table limit 10").show(10)

    /**
      * 每个一级类别被看到的次数
      */
    val targetInviewCount =
      spark.sql(
        s"""
           |SELECT ${target}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS ${target}_inview_count
           |FROM ${joinInviewTable}
           |group by ${target}
        """.stripMargin)
        .cache()
    targetInviewCount.createOrReplaceTempView(s"${target}_inview_count_table")
    //    println(s"每个一级类别被看到的次数：${target}")
    //    println(s"数量为：" + targetInviewCount.count())
    //    spark.sql(s"select * from ${target}_inview_count_table limit 10").show(10)

    /**
      * 各类用户点击所有的一级品类的次数
      */
    val typeUserClickTargetsCount =
      spark.sql(
        s"""
           |SELECT
           | ccct.${target} AS user_type,
           | sum(ucccts.user_click_${target}s_count) AS type_user_click_${target}s_counts
           |FROM
           | ${target}_click_count_table ccct,
           | user_click_${target}s_count_table ucccts,
           | user_click_${target}_count_table uccct
           |WHERE ccct.${target} = uccct.${target}
           |AND uccct.${Constants.TARGET_DEVICEID_TABLE_NAME} = ucccts.${Constants.TARGET_DEVICEID_TABLE_NAME}
           |GROUP BY ccct.${target}
        """.stripMargin)
        .cache()
    typeUserClickTargetsCount.createOrReplaceTempView(s"type_user_click_${target}s_count_table")
    //    println(s"类别用户点击所有一级品类次数：${target}")
    //    println(s"数量为：" + typeUserClickTargetsCount.count())
    //    spark.sql(s"select * from type_user_click_${target}s_count_table limit 10").show(10)

    //

    /**
      * 计算 CS
      */
    val userTargetCS =
      spark.sql(
        s"""
           |SELECT
           | uccct.${Constants.TARGET_DEVICEID_TABLE_NAME},
           | uccct.${target},
           | (wilson(ucccts.user_click_${target}s_count,uccct.user_click_${target}_count,1) / wilson(tuccct.type_user_click_${target}s_counts,ccct.${target}_click_count,1)) AS CS
           |FROM
           |  user_click_${target}_count_table uccct,
           |  user_click_${target}s_count_table ucccts,
           |  ${target}_click_count_table ccct,
           |  type_user_click_${target}s_count_table tuccct
           |WHERE uccct.${Constants.TARGET_DEVICEID_TABLE_NAME} = ucccts.${Constants.TARGET_DEVICEID_TABLE_NAME}
           |AND uccct.${target} = ccct.${target}
           |AND uccct.${target} = tuccct.user_type
    """.stripMargin)
    userTargetCS.cache()
    //    println(s"cs计算结果：${target}")
    //    println(s"数量为：" + userTargetCS.count())
    //    userTargetCS.show(10)
    //    userTargetCS.createTempView(s"user_${target}_cs")

    /**
      * 计算 cs_smooth
      */
    //    val userTargetCsSmooth =
    //      spark.sql(
    //        s"""
    //           |SELECT
    //           | uccct.${Constants.TARGET_DEVICEID_TABLE_NAME},
    //           | uccct.${target},
    //           | (((uccct.user_click_${target}_count + 3) / (ucccts.user_click_${target}s_count + (3 * (tuccct.type_user_click_${target}s_counts / ccct.${target}_click_count)))) / (ccct.${target}_click_count / tuccct.type_user_click_${target}s_counts)) AS CS_SMOOTH
    //           |FROM
    //           |  user_click_${target}_count_table uccct,
    //           |  user_click_${target}s_count_table ucccts,
    //           |  ${target}_click_count_table ccct,
    //           |  type_user_click_${target}s_count_table tuccct
    //           |WHERE uccct.${Constants.TARGET_DEVICEID_TABLE_NAME} = ucccts.${Constants.TARGET_DEVICEID_TABLE_NAME}
    //           |AND uccct.${target} = ccct.${target}
    //           |AND uccct.${target} = tuccct.user_type
    //    """.stripMargin)

    //    userTargetCsSmooth.createTempView(s"user_${target}_cs_smooth")

    /**
      * 计算 CK
      */
    //    val userTargetCK =
    //      spark.sql(
    //        s"""
    //           |SELECT
    //           | uccct.${Constants.TARGET_DEVICEID_TABLE_NAME},
    //           | uccct.${target},
    //           | (wilson(uicct.user_inview_${target}_count,uccct.user_click_${target}_count,2) / wilson(cict.${target}_inview_count,ccct.${target}_click_count,2)) AS CK
    //           |FROM
    //           |  user_click_${target}_count_table uccct,
    //           |  user_inview_${target}_count_table uicct,
    //           |  ${target}_click_count_table ccct,
    //           |  ${target}_inview_count_table cict
    //           |WHERE uccct.${Constants.TARGET_DEVICEID_TABLE_NAME} = uicct.${Constants.TARGET_DEVICEID_TABLE_NAME}
    //           |AND uccct.${target} = uicct.${target}
    //           |AND uccct.${target} = ccct.${target}
    //           |AND uccct.${target} = cict.${target}
    //        """.stripMargin)

    /**
      * 计算 CK，分母使用威尔逊，分子直接使用比值，但是要关联出所有inview的数据
      */
    val sql =
      s"""
         |select
         |${Constants.TARGET_DEVICEID_TABLE_NAME},
         |${target},
         |case when CK is null then 0 else CK end as CK
         | from
         |(select ${Constants.TARGET_DEVICEID_TABLE_NAME},
         |${target},
         |(((user_click_${target}_count+0.1)/user_inview_${target}_count) / wilson(${target}_inview_count,${target}_click_count,2)) AS CK
         | from
         |(select
         |uicct.${Constants.TARGET_DEVICEID_TABLE_NAME},
         |uicct.${target},
         |uicct.user_inview_${target}_count,
         |case when uccct.user_click_${target}_count is null then 0 else uccct.user_click_${target}_count end as user_click_${target}_count,
         |case when cict.${target}_inview_count is null then 0 else cict.${target}_inview_count end as ${target}_inview_count,
         |case when ccct.${target}_click_count is null then 0 else ccct.${target}_click_count end as ${target}_click_count
         | from
         |user_inview_${target}_count_table uicct
         |left join
         |user_click_${target}_count_table uccct
         |on uicct.${target}=uccct.${target} and uicct.${Constants.TARGET_DEVICEID_TABLE_NAME}=uccct.${Constants.TARGET_DEVICEID_TABLE_NAME}
         |left join
         |${target}_inview_count_table cict
         |on uicct.${target}=cict.${target}
         |left join
         |${target}_click_count_table ccct
         |on uicct.${target}=ccct.${target}
         |) t
         |) t
        """.stripMargin
    println(sql)
    val userTargetCK = spark.sql(sql)
    userTargetCK.cache()

    //ck计算使用前一天的校验值，注意关联时去除引号
    //    val sql =
    //      s"""
    //         |select
    //         |${Constants.TARGET_DEVICEID_TABLE_NAME},
    //         |${target},
    //         |case when CK is null then 0 else CK end as CK
    //         | from
    //         |(select ${Constants.TARGET_DEVICEID_TABLE_NAME},
    //         |${target},
    //         |(wilson(user_inview_${target}_count,(user_click_${target}_count+avg),2) / wilson(${target}_inview_count,${target}_click_count,2)) AS CK
    //         | from
    //         |(select
    //         |uicct.${Constants.TARGET_DEVICEID_TABLE_NAME},
    //         |uicct.${target},
    //         |uicct.user_inview_${target}_count,
    //         |case when uccct.user_click_${target}_count is null then 0 else uccct.user_click_${target}_count end as user_click_${target}_count,
    //         |case when cict.${target}_inview_count is null then 0 else cict.${target}_inview_count end as ${target}_inview_count,
    //         |case when ccct.${target}_click_count is null then 0 else ccct.${target}_click_count end as ${target}_click_count,
    //         |upf.avg
    //         | from
    //         |user_inview_${target}_count_table uicct
    //         |left join
    //         |user_click_${target}_count_table uccct
    //         |on uicct.${target}=uccct.${target} and uicct.${Constants.TARGET_DEVICEID_TABLE_NAME}=uccct.${Constants.TARGET_DEVICEID_TABLE_NAME}
    //         |left join
    //         |${target}_inview_count_table cict
    //         |on uicct.${target}=cict.${target}
    //         |left join
    //         |${target}_click_count_table ccct
    //         |on uicct.${target}=ccct.${target}
    //         |left join
    //         |(
    //         |select split(target,'_')[1] as target,avg from userprofile.user_profile_feature_stat
    //         |where pday=${yesterday}
    //         |and split(target,'_')[0]='${targetJsonName}'
    //         |and split(target,'_')[2]='ck'
    //         |) upf
    //         |on replace(uicct.${target},'\"','')=upf.target
    //         |) t
    //         |) t
    //    """.stripMargin
    //    println(sql)
    //    val userTargetCK = spark.sql(sql)
    //    userTargetCK.cache()
    //    println(s"ck计算结果：${target}")
    //    println(s"数量为：" + userTargetCK.count())
    //    userTargetCK.show(10)
    //    userTargetCK.createTempView(s"user_${target}_ck")

    //如果是tag取前500
    if (targetJsonName == "tag") {
      val userTargetCSRdd = userTargetCS.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          .replaceAll(""""""", "")
        val csKeyStr = s"${targetJsonName}_${targetTrim}_cs"
        val csValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + csKeyStr + ":" + csValueStr
        val result = deviceId + "||" + csKeyStr + ":" + csValueStr
        (deviceId, csValueStr, result)
      })
      import spark.implicits._
      val tag_df_cs = userTargetCSRdd.toDF("key", "value", "result")
      tag_df_cs.createOrReplaceTempView("tag_order_cs")
      val result_cs = spark.sql(
        """
          |select key,result from
          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_cs
          |) t0
          |where rn<=500
        """.stripMargin)
        .rdd
        .map(_.get(1).toString)

      //对于tag cs大于500的部分作为dislikeTag
      //      val dislikeTag_cs = spark.sql(
      //        """
      //          |select key,result from
      //          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_cs
      //          |) t0
      //          |where rn>500
      //        """.stripMargin)
      //        .rdd
      //        .map{x=>{
      //          val str=x.get(1).toString
      //          val deviceId = str.split("\\|\\|")(0).trim
      //          val feature = str.split("\\|\\|")(1).trim
      //          val dislike_feature="dislikeT"+feature.substring(1)
      //          val result = deviceId + "||" + dislike_feature
      //          result
      //        }}


      val userTargetCKRdd = userTargetCK.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          //          .substring(1, target.length - 1)  from不适用
          .replaceAll(""""""", "")
        val ckKeyStr = s"${targetJsonName}_${targetTrim}_ck"
        val ckValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + ckKeyStr + ":" + ckValueStr
        val result = deviceId + "||" + ckKeyStr + ":" + ckValueStr
        (deviceId, ckValueStr, result)
      })
      val tag_df_ck = userTargetCKRdd.toDF("key", "value", "result")
      tag_df_ck.createOrReplaceTempView("tag_order_ck")
      val result_ck = spark.sql(
        """
          |select key,result from
          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_ck
          |) t0
          |where rn<=500
        """.stripMargin)
        .rdd
        .map(_.get(1).toString)

      //对于tag cs大于500的部分作为dislikeTag
      //      val dislikeTag_ck = spark.sql(
      //        """
      //          |select key,result from
      //          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_ck
      //          |) t0
      //          |where rn>500
      //        """.stripMargin)
      //        .rdd
      //        .map(x=>{
      //          val str=x.get(1).toString
      //          val deviceId = str.split("\\|\\|")(0).trim
      //          val feature = str.split("\\|\\|")(1).trim
      //          val dislike_feature="dislikeT"+feature.substring(1)
      //          val result = deviceId + "||" + dislike_feature
      //          result
      //        })
      //
      //
      //      //      val userTargetCSSmoothRdd = userTargetCsSmooth.rdd.map(row => {
      //      //        val deviceId = row.get(0).toString
      //      //        val target = row.get(1).toString
      //      //        val targetTrim = target.substring(1, target.length - 1)
      //      //        val group_key = s"${targetJsonName}_${targetTrim}"
      //      //        val csSmoothKeyStr = s"${targetJsonName}_${targetTrim}_cs_smooth"
      //      //        val csSmoothValueStr = row.get(2).toString
      //      //        //      deviceId + "||" + group_key + "," + csSmoothKeyStr + ":" + csSmoothValueStr
      //      //        val result = deviceId + "||" + csSmoothKeyStr + ":" + csSmoothValueStr
      //      //        (deviceId, csSmoothValueStr, result)
      //      //      })
      //      //      val tag_df_csSmooth = userTargetCSSmoothRdd.toDF("key", "value", "result")
      //      //      tag_df_csSmooth.createOrReplaceTempView("tag_order_csSmooth")
      //      //      val result_csSmooth = spark.sql(
      //      //        """
      //      //          |select key,result from
      //      //          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_csSmooth
      //      //          |) t0
      //      //          |where rn<=300
      //      //        """.stripMargin)
      //      //        .rdd
      //      //        .map(_.get(1).toString)
      //
      userClickTargetCount.unpersist()
      userClickTargetsCount.unpersist()
      userInviewTargetCount.unpersist()
      targetClickCount.unpersist()
      targetInviewCount.unpersist()
      typeUserClickTargetsCount.unpersist()
      userTargetCS.unpersist()
      userTargetCK.unpersist()
      result_cs.union(result_ck)
      //        .union(dislikeTag_cs).union(dislikeTag_ck)
      //      //        .union(result_csSmooth)
    }
    //from取前100
    else if (targetJsonName == "from") {
      val userTargetCSRdd = userTargetCS.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          .replaceAll(""""""", "")
        val csKeyStr = s"${targetJsonName}_${targetTrim}_cs"
        val csValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + csKeyStr + ":" + csValueStr
        val result = deviceId + "||" + csKeyStr + ":" + csValueStr
        (deviceId, csValueStr, result)
      })
      import spark.implicits._
      val tag_df_cs = userTargetCSRdd.toDF("key", "value", "result")
      tag_df_cs.createOrReplaceTempView("tag_order_cs")
      val result_cs = spark.sql(
        """
          |select key,result from
          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_cs
          |) t0
          |where rn<=100
        """.stripMargin)
        .rdd
        .map(_.get(1).toString)
      val userTargetCKRdd = userTargetCK.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          //          .substring(1, target.length - 1)  from不适用
          .replaceAll(""""""", "")
        val ckKeyStr = s"${targetJsonName}_${targetTrim}_ck"
        val ckValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + ckKeyStr + ":" + ckValueStr
        val result = deviceId + "||" + ckKeyStr + ":" + ckValueStr
        (deviceId, ckValueStr, result)
      })
      val tag_df_ck = userTargetCKRdd.toDF("key", "value", "result")
      tag_df_ck.createOrReplaceTempView("tag_order_ck")
      val result_ck = spark.sql(
        """
          |select key,result from
          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_ck
          |) t0
          |where rn<=100
        """.stripMargin)
        .rdd
        .map(_.get(1).toString)
      userClickTargetCount.unpersist()
      userClickTargetsCount.unpersist()
      userInviewTargetCount.unpersist()
      targetClickCount.unpersist()
      targetInviewCount.unpersist()
      typeUserClickTargetsCount.unpersist()
      userTargetCS.unpersist()
      userTargetCK.unpersist()
      result_cs.union(result_ck)
    }
    //categoryDfrom取前200
    else if (targetJsonName == "categoryDfrom") {
      val userTargetCSRdd = userTargetCS.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          .replaceAll(""""""", "")
        val csKeyStr = s"${targetJsonName}_${targetTrim}_cs"
        val csValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + csKeyStr + ":" + csValueStr
        val result = deviceId + "||" + csKeyStr + ":" + csValueStr
        (deviceId, csValueStr, result)
      })
      import spark.implicits._
      val tag_df_cs = userTargetCSRdd.toDF("key", "value", "result")
      tag_df_cs.createOrReplaceTempView("tag_order_cs")
      val result_cs = spark.sql(
        """
          |select key,result from
          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_cs
          |) t0
          |where rn<=200
        """.stripMargin)
        .rdd
        .map(_.get(1).toString)
      val userTargetCKRdd = userTargetCK.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          //          .substring(1, target.length - 1)  from不适用
          .replaceAll(""""""", "")
        val ckKeyStr = s"${targetJsonName}_${targetTrim}_ck"
        val ckValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + ckKeyStr + ":" + ckValueStr
        val result = deviceId + "||" + ckKeyStr + ":" + ckValueStr
        (deviceId, ckValueStr, result)
      })
      val tag_df_ck = userTargetCKRdd.toDF("key", "value", "result")
      tag_df_ck.createOrReplaceTempView("tag_order_ck")
      val result_ck = spark.sql(
        """
          |select key,result from
          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_ck
          |) t0
          |where rn<=200
        """.stripMargin)
        .rdd
        .map(_.get(1).toString)
      userClickTargetCount.unpersist()
      userClickTargetsCount.unpersist()
      userInviewTargetCount.unpersist()
      targetClickCount.unpersist()
      targetInviewCount.unpersist()
      typeUserClickTargetsCount.unpersist()
      userTargetCS.unpersist()
      userTargetCK.unpersist()
      result_cs.union(result_ck)
    }
    //t1000排序取前500
    //    else if (targetJsonName == "t1000") {
    //      val userTargetCSRdd = userTargetCS.rdd.map(row => {
    //        val deviceId = row.get(0).toString
    //        val target = row.get(1).toString
    //        val targetTrim = target.substring(1, target.length - 1)
    //        val group_key = s"${targetJsonName}_${targetTrim}"
    //        val csKeyStr = s"${targetJsonName}_${targetTrim}_cs"
    //        val csValueStr = row.get(2).toString
    //        //      deviceId + "||" + group_key + "," + csKeyStr + ":" + csValueStr
    //        val result = deviceId + "||" + csKeyStr + ":" + csValueStr
    //        (deviceId, csValueStr, result)
    //      })
    //      import spark.implicits._
    //      val tag_df_cs = userTargetCSRdd.toDF("key", "value", "result")
    //      tag_df_cs.createOrReplaceTempView("tag_order_cs")
    //      val result_cs = spark.sql(
    //        """
    //          |select key,result from
    //          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_cs
    //          |) t0
    //          |where rn<=500
    //        """.stripMargin)
    //        .rdd
    //        .map(_.get(1).toString)
    //
    //
    //      val userTargetCKRdd = userTargetCK.rdd.map(row => {
    //        val deviceId = row.get(0).toString
    //        val target = row.get(1).toString
    //        val targetTrim = target.substring(1, target.length - 1)
    //        val group_key = s"${targetJsonName}_${targetTrim}"
    //        val ckKeyStr = s"${targetJsonName}_${targetTrim}_ck"
    //        val ckValueStr = row.get(2).toString
    //        //      deviceId + "||" + group_key + "," + ckKeyStr + ":" + ckValueStr
    //        val result = deviceId + "||" + ckKeyStr + ":" + ckValueStr
    //        (deviceId, ckValueStr, result)
    //      })
    //      val tag_df_ck = userTargetCKRdd.toDF("key", "value", "result")
    //      tag_df_ck.createOrReplaceTempView("tag_order_ck")
    //      val result_ck = spark.sql(
    //        """
    //          |select key,result from
    //          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_ck
    //          |) t0
    //          |where rn<=500
    //        """.stripMargin)
    //        .rdd
    //        .map(_.get(1).toString)
    //
    //
    //      val userTargetCSSmoothRdd = userTargetCsSmooth.rdd.map(row => {
    //        val deviceId = row.get(0).toString
    //        val target = row.get(1).toString
    //        val targetTrim = target.substring(1, target.length - 1)
    //        val group_key = s"${targetJsonName}_${targetTrim}"
    //        val csSmoothKeyStr = s"${targetJsonName}_${targetTrim}_cs_smooth"
    //        val csSmoothValueStr = row.get(2).toString
    //        //      deviceId + "||" + group_key + "," + csSmoothKeyStr + ":" + csSmoothValueStr
    //        val result = deviceId + "||" + csSmoothKeyStr + ":" + csSmoothValueStr
    //        (deviceId, csSmoothValueStr, result)
    //      })
    //      val tag_df_csSmooth = userTargetCSSmoothRdd.toDF("key", "value", "result")
    //      tag_df_csSmooth.createOrReplaceTempView("tag_order_csSmooth")
    //      val result_csSmooth = spark.sql(
    //        """
    //          |select key,result from
    //          |(select key,value,result,row_number() over (partition by key order by value desc) rn from tag_order_csSmooth
    //          |) t0
    //          |where rn<=500
    //        """.stripMargin)
    //        .rdd
    //        .map(_.get(1).toString)
    //
    //      result_cs.union(result_ck).union(result_csSmooth)
    //        }
    else {
      val userTargetCSRdd = userTargetCS.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          //          .substring(1, target.length - 1)
          .replaceAll(""""""", "")
        //      val group_key = s"${targetJsonName}_${targetTrim}"
        val csKeyStr = s"${targetJsonName}_${targetTrim}_cs"
        val csValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + csKeyStr + ":" + csValueStr
        deviceId + "||" + csKeyStr + ":" + csValueStr
      })


      val userTargetCKRdd: RDD[String] = userTargetCK.rdd.map(row => {
        try {
          val deviceId = row.get(0).toString
          val target = row.get(1).toString
          val targetTrim = target
            //          .substring(1, target.length - 1)  form不适用
            .replaceAll(""""""", "")
          val group_key = s"${targetJsonName}_${targetTrim}"
          val ckKeyStr = s"${targetJsonName}_${targetTrim}_ck"
          val ckValueStr =
            try {
              row.get(2).toString
            }
            catch {
              case e => e.printStackTrace()
                println(row.toString())
                0
            }
          //      deviceId + "||" + group_key + "," + ckKeyStr + ":" + ckValueStr
          deviceId + "||" + ckKeyStr + ":" + ckValueStr
        }
        catch {
          case e => e.printStackTrace()
            println(row.toString())
            "null" + "||" + "null" + 0
        }
      }
      )

      //      val userTargetCSSmoothRdd: RDD[String] = userTargetCsSmooth.rdd.map(row => {
      //        val deviceId = row.get(0).toString
      //        val target = row.get(1).toString
      //        val targetTrim = target.substring(1, target.length - 1)
      //        val group_key = s"${targetJsonName}_${targetTrim}"
      //        val csSmoothKeyStr = s"${targetJsonName}_${targetTrim}_cs_smooth"
      //        val csSmoothValueStr = row.get(2).toString
      //        //      deviceId + "||" + group_key + "," + csSmoothKeyStr + ":" + csSmoothValueStr
      //        deviceId + "||" + csSmoothKeyStr + ":" + csSmoothValueStr
      //      })
      userClickTargetCount.unpersist()
      userClickTargetsCount.unpersist()
      userInviewTargetCount.unpersist()
      targetClickCount.unpersist()
      targetInviewCount.unpersist()
      typeUserClickTargetsCount.unpersist()
      userTargetCS.unpersist()
      userTargetCK.unpersist()
      userTargetCSRdd.union(userTargetCKRdd)
      //        .union(userTargetCSSmoothRdd)
    }
    //    val result =
    //      spark.sql(
    //        s"""
    //          |select
    //          | uccs.${Constants.TARGET_DEVICEID_TABLE_NAME}, uccs.${target}, uccs.CS, ucck.CK, uccss.CS_SMOOTH
    //          |from
    //          | user_${target}_cs uccs,
    //          | user_${target}_ck ucck,
    //          | user_${target}_cs_smooth uccss
    //          |WHERE uccs.${Constants.TARGET_DEVICEID_TABLE_NAME} = ucck.${Constants.TARGET_DEVICEID_TABLE_NAME}
    //          |AND ucck.${Constants.TARGET_DEVICEID_TABLE_NAME} = uccss.${Constants.TARGET_DEVICEID_TABLE_NAME}
    //          |AND uccs.${target} = ucck.${target}
    //          |AND ucck.${target} = uccss.${target}
    //        """.stripMargin).cache()
  }

  /**
    * 区别于新闻用户画像，视频画像的feature取值不同
    */
  def getVideoTargetResult(joinClickTableName: String, joinInviewTable: String, target: String, targetJsonName: String, spark: SparkSession): RDD[String] = {

    /**
      * 每个用户点击各个一级品类的次数
      * 866401030845468, "育儿", 7
      * bd7cf281302717fa, "娱乐", 25
      */
    spark.udf.register("wilson", wilson)

    val userClickTargetCount =
      spark.sql(
        s"""
           |SELECT ${Constants.TARGET_DEVICEID_TABLE_NAME}, ${target}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS user_click_${target}_count
           |FROM ${joinClickTableName}
           |GROUP BY ${Constants.TARGET_DEVICEID_TABLE_NAME}, ${target}
        """.stripMargin)
        .cache()
    userClickTargetCount.createOrReplaceTempView(s"user_click_${target}_count_table")
    println(s"用户点击各个一级feature：${target}")
    println(s"数量为：" + userClickTargetCount.count())
    spark.sql(s"select * from user_click_${target}_count_table limit 10").show(10)
    /**
      * 每个用户点击所有一级品类的次数
      * c864071033940939, 282
      * ae46bca04c71cfb8, 5
      */
    val userClickTargetsCount =
      spark.sql(
        s"""
           |SELECT ${Constants.TARGET_DEVICEID_TABLE_NAME}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS user_click_${target}s_count
           |FROM ${joinClickTableName}
           |GROUP BY ${Constants.TARGET_DEVICEID_TABLE_NAME}
        """.stripMargin)
        .cache()
    userClickTargetsCount.createTempView(s"user_click_${target}s_count_table")
    println(s"用户点击所有一级类别：${target}")
    println(s"数量为：" + userClickTargetsCount.count())
    spark.sql(s"select * from user_click_${target}s_count_table limit 10").show(10)

    /**
      * 每个用户看到各个一级品类的次数
      */
    val userInviewTargetCount =
      spark.sql(
        s"""
           |SELECT ${Constants.TARGET_DEVICEID_TABLE_NAME}, ${target}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS user_inview_${target}_count
           |FROM ${joinInviewTable}
           |GROUP BY ${Constants.TARGET_DEVICEID_TABLE_NAME}, ${target}
        """.stripMargin)
        .cache()
    userInviewTargetCount.createOrReplaceTempView(s"user_inview_${target}_count_table")
    println(s"用户曝光各个一级feature：${target}")
    println(s"数量为：" + userInviewTargetCount.count())
    spark.sql(s"select * from user_inview_${target}_count_table limit 10").show(10)

    /**
      * 每个一级类别被点击的总次数
      * "科学", 1628
      * "财经", 1604
      */
    val targetClickCount =
      spark.sql(
        s"""
           |SELECT ${target}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS ${target}_click_count
           |FROM ${joinClickTableName}
           |group by ${target}
        """.stripMargin)
        .cache()
    targetClickCount.createOrReplaceTempView(s"${target}_click_count_table")
    println(s"每个一级类别被点击总次数：${target}")
    println(s"数量为：" + targetClickCount.count())
    spark.sql(s"select * from ${target}_click_count_table limit 10").show(10)

    /**
      * 每个一级类别被看到的次数
      */
    val targetInviewCount =
      spark.sql(
        s"""
           |SELECT ${target}, SUM(${Constants.TARGET_SCORE_TABLE_NAME}) AS ${target}_inview_count
           |FROM ${joinInviewTable}
           |group by ${target}
        """.stripMargin)
        .cache()
    targetInviewCount.createOrReplaceTempView(s"${target}_inview_count_table")
    println(s"每个一级类别被看到的次数：${target}")
    println(s"数量为：" + targetInviewCount.count())
    spark.sql(s"select * from ${target}_inview_count_table limit 10").show(10)

    /**
      * 各类用户点击所有的一级品类的次数
      */
    val typeUserClickTargetsCount =
      spark.sql(
        s"""
           |SELECT
           | ccct.${target} AS user_type,
           | sum(ucccts.user_click_${target}s_count) AS type_user_click_${target}s_counts
           |FROM
           | ${target}_click_count_table ccct,
           | user_click_${target}s_count_table ucccts,
           | user_click_${target}_count_table uccct
           |WHERE ccct.${target} = uccct.${target}
           |AND uccct.${Constants.TARGET_DEVICEID_TABLE_NAME} = ucccts.${Constants.TARGET_DEVICEID_TABLE_NAME}
           |GROUP BY ccct.${target}
        """.stripMargin)
        .cache()
    typeUserClickTargetsCount.createOrReplaceTempView(s"type_user_click_${target}s_count_table")
    println(s"类别用户点击所有一级品类次数：${target}")
    println(s"数量为：" + typeUserClickTargetsCount.count())
    spark.sql(s"select * from type_user_click_${target}s_count_table limit 10").show(10)

    //

    /**
      * 计算 CS
      */
    val userTargetCS =
      spark.sql(
        s"""
           |SELECT
           | uccct.${Constants.TARGET_DEVICEID_TABLE_NAME},
           | uccct.${target},
           | (wilson(ucccts.user_click_${target}s_count,uccct.user_click_${target}_count,1) / wilson(tuccct.type_user_click_${target}s_counts,ccct.${target}_click_count,1)) AS CS
           |FROM
           |  user_click_${target}_count_table uccct,
           |  user_click_${target}s_count_table ucccts,
           |  ${target}_click_count_table ccct,
           |  type_user_click_${target}s_count_table tuccct
           |WHERE uccct.${Constants.TARGET_DEVICEID_TABLE_NAME} = ucccts.${Constants.TARGET_DEVICEID_TABLE_NAME}
           |AND uccct.${target} = ccct.${target}
           |AND uccct.${target} = tuccct.user_type
    """.stripMargin)
    println(s"cs计算结果：${target}")
    println(s"数量为：" + userTargetCS.count())
    userTargetCS.show(10)
    //    userTargetCS.createTempView(s"user_${target}_cs")

    /**
      * 计算 CK
      */
    val userTargetCK =
      spark.sql(
        s"""
           |SELECT
           | uccct.${Constants.TARGET_DEVICEID_TABLE_NAME},
           | uccct.${target},
           | (wilson(uicct.user_inview_${target}_count,uccct.user_click_${target}_count,2) / wilson(cict.${target}_inview_count,ccct.${target}_click_count,2)) AS CK
           |FROM
           |  user_click_${target}_count_table uccct,
           |  user_inview_${target}_count_table uicct,
           |  ${target}_click_count_table ccct,
           |  ${target}_inview_count_table cict
           |WHERE uccct.${Constants.TARGET_DEVICEID_TABLE_NAME} = uicct.${Constants.TARGET_DEVICEID_TABLE_NAME}
           |AND uccct.${target} = uicct.${target}
           |AND uccct.${target} = ccct.${target}
           |AND uccct.${target} = cict.${target}
    """.stripMargin)
    userTargetCK.cache()
    println(s"ck计算结果：${target}")
    println(s"数量为：" + userTargetCK.count())
    userTargetCK.show(10)
    //    userTargetCK.createTempView(s"user_${target}_ck")

    //video的tag和outertag取前300，from取前100，outercate取前30
    if (targetJsonName == Constants.TARGET_TAG_JSON_NAME) {
      getTopN(userTargetCS, userTargetCK, Constants.TARGET_TAG_JSON_NAME, spark, 300)
    } else if (targetJsonName == Constants.TARGET_OUTERTAG_JSON_NAME) {
      getTopN(userTargetCS, userTargetCK, Constants.TARGET_OUTERTAG_JSON_NAME, spark, 300)
    } else if (targetJsonName == Constants.TARGET_OUTERCATE_JSON_NAME) {
      getTopN(userTargetCS, userTargetCK, Constants.TARGET_OUTERCATE_JSON_NAME, spark, 30)
    } else if (targetJsonName == Constants.TARGET_DFROM_JSON_NAME) {
      getTopN(userTargetCS, userTargetCK, Constants.TARGET_DFROM_JSON_NAME, spark, 100)
    } else {
      val userTargetCSRdd = userTargetCS.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          //          .substring(1, target.length - 1)
          .replaceAll(""""""", "")
        //      val group_key = s"${targetJsonName}_${targetTrim}"
        val csKeyStr = s"${targetJsonName}_${targetTrim}_cs"
        val csValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + csKeyStr + ":" + csValueStr
        deviceId + "||" + csKeyStr + ":" + csValueStr
      })

      val userTargetCKRdd: RDD[String] = userTargetCK.rdd.map(row => {
        val deviceId = row.get(0).toString
        val target = row.get(1).toString
        val targetTrim = target
          //          .substring(1, target.length - 1)  form不适用
          .replaceAll(""""""", "")
        val group_key = s"${targetJsonName}_${targetTrim}"
        val ckKeyStr = s"${targetJsonName}_${targetTrim}_ck"
        val ckValueStr = row.get(2).toString
        //      deviceId + "||" + group_key + "," + ckKeyStr + ":" + ckValueStr
        deviceId + "||" + ckKeyStr + ":" + ckValueStr
      })
      userTargetCSRdd.union(userTargetCKRdd)
    }
  }

  val wilson = (num1: Double, num2: Double, flag: Int) => {
    try {
      import org.apache.commons.math3.stat.interval.WilsonScoreInterval
      val wsi = new WilsonScoreInterval
      val interval = wsi.createInterval(Math.max(Math.ceil(num1).toInt, Math.ceil(num2).toInt), Math.ceil(num2).toInt, 0.95)
      interval.getLowerBound
    } catch {
      case e => e.printStackTrace()
        println(flag + "..." + num1 + "..." + num2)
        import org.apache.commons.math3.stat.interval.WilsonScoreInterval
        val wsi = new WilsonScoreInterval
        wsi.createInterval(Math.max(Math.ceil(num1).toInt, Math.ceil(num2).toInt), Math.ceil(num2).toInt, 0.95) getLowerBound
    }
  }

  def getFinalResultRdd(unionResultRdd: RDD[String], clickRdd: RDD[(String, String)], inviewRdd: RDD[(String, String)]): RDD[String] = {
    unionResultRdd.map(x => {
      val deviceId = x.split("\\|\\|")(0).trim
      val str = x.split("\\|\\|")(1).trim
      (deviceId, str)
    })
      //添加用户点击统计和曝光统计
      .union(clickRdd)
      .union(inviewRdd)
      //          unionResultRdd.map(x => {
      //            val deviceId = x.split("\\|\\|")(0).trim
      //            val str = x.split("\\|\\|")(1).trim
      //            val group_key = str.split(",")(0)
      //            val value = str.split(",")(1)
      //            ((deviceId, group_key), value)
      //          })
      .reduceByKey(_ + "," + _) // (10000,category_娱乐_cs:1.0^Acategory_娱乐_cs_smooth:1.0^AsecondCate_娱乐/港台娱乐_cs:1.0^AsecondCate_娱乐/港台娱乐_cs_smooth:1.0^Atag_范伟_cs:0.5^Atag_内地娱乐_cs:0.5^Atag_范伟_cs_smooth:0.8^Atag_内地娱乐_cs_smooth:0.8^At64_60_cs:0.5^At64_21_cs:0.5^At64_60_cs_smooth:0.8^At64_21_cs_smooth:0.8^At128_20_cs:0.3333333333333333^At128_25_cs:0.3333333333333333^At128_81_cs:0.3333333333333333^At128_20_cs_smooth:0.6666666666666666^At128_25_cs_smooth:0.6666666666666666^At128_81_cs_smooth:0.6666666666666666^At256_209_cs:0.5^At256_63_cs:0.5^At256_209_cs_smooth:0.8^At256_63_cs_smooth:0.8^At512_418_cs:0.5^At512_104_cs:0.5^At512_418_cs_smooth:0.8^At512_104_cs_smooth:0.8^At1000_418_cs:0.5^At1000_104_cs:0.5^At1000_418_cs_smooth:0.8^At1000_104_cs_smooth:0.8)
      //            .map(tuple => {
      //            val deviceId = tuple._1._1
      //            val str = tuple._2
      //      .reduceByKey(_ + "," + _) // (10000,category_娱乐_cs:1.0^Acategory_娱乐_cs_smooth:1.0^AsecondCate_娱乐/港台娱乐_cs:1.0^AsecondCate_娱乐/港台娱乐_cs_smooth:1.0^Atag_范伟_cs:0.5^Atag_内地娱乐_cs:0.5^Atag_范伟_cs_smooth:0.8^Atag_内地娱乐_cs_smooth:0.8^At64_60_cs:0.5^At64_21_cs:0.5^At64_60_cs_smooth:0.8^At64_21_cs_smooth:0.8^At128_20_cs:0.3333333333333333^At128_25_cs:0.3333333333333333^At128_81_cs:0.3333333333333333^At128_20_cs_smooth:0.6666666666666666^At128_25_cs_smooth:0.6666666666666666^At128_81_cs_smooth:0.6666666666666666^At256_209_cs:0.5^At256_63_cs:0.5^At256_209_cs_smooth:0.8^At256_63_cs_smooth:0.8^At512_418_cs:0.5^At512_104_cs:0.5^At512_418_cs_smooth:0.8^At512_104_cs_smooth:0.8^At1000_418_cs:0.5^At1000_104_cs:0.5^At1000_418_cs_smooth:0.8^At1000_104_cs_smooth:0.8)
      //添加用户点击数和曝光数

      .map(x =>
      x._1 + "\t" + x._2
    )
    //      .map(tuple => {
    //      val deviceId = tuple._1._1
    //      val str = tuple._2
    //              val jsonObject = new JSONObject()
    //              jsonObject.put(s"${Constants.TARGET_DEVICEID_JSON_NAME}", deviceId)
    //              val kvs = str.split(" ")
    //              for(kv <- kvs) {
    //                jsonObject.put(s"${kv.split(":")(0).trim}", s"${kv.split(":")(1).trim}")
    //              }
    //              jsonObject.toJSONString
    //      deviceId + "\t" + str
    //    })
  }

  /**
    * 在调用getTargetResult方法或者getVideoTargetResult方法时，不同的feature可能取不同的topN
    */
  def getTopN(userTargetCS: DataFrame, userTargetCK: DataFrame, targetJsonName: String, spark: SparkSession, topN: Int): RDD[String] = {
    val userTargetCSRdd = userTargetCS.rdd.map(row => {
      val deviceId = row.get(0).toString
      val target = row.get(1).toString
      val targetTrim = target
        .replaceAll(""""""", "")
      val csKeyStr = s"${targetJsonName}_${targetTrim}_cs"
      val csValueStr = row.get(2).toString
      //      deviceId + "||" + group_key + "," + csKeyStr + ":" + csValueStr
      val result = deviceId + "||" + csKeyStr + ":" + csValueStr
      (deviceId, csValueStr, result)
    })
    import spark.implicits._
    val tag_df_cs = userTargetCSRdd.toDF("key", "value", "result")
    tag_df_cs.createOrReplaceTempView(s"${targetJsonName}_order_cs")
    val result_cs = spark.sql(
      s"""
         |select key,result from
         |(select key,value,result,row_number() over (partition by key order by value desc) rn from ${targetJsonName}_order_cs
         |) t0
         |where rn<=${topN}
      """.stripMargin)
      .rdd
      .map(_.get(1).toString)
    val userTargetCKRdd = userTargetCK.rdd.map(row => {
      val deviceId = row.get(0).toString
      val target = row.get(1).toString
      val targetTrim = target
        //          .substring(1, target.length - 1)  from不适用
        .replaceAll(""""""", "")
      val ckKeyStr = s"${targetJsonName}_${targetTrim}_ck"
      val ckValueStr = row.get(2).toString
      //      deviceId + "||" + group_key + "," + ckKeyStr + ":" + ckValueStr
      val result = deviceId + "||" + ckKeyStr + ":" + ckValueStr
      (deviceId, ckValueStr, result)
    })
    val tag_df_ck = userTargetCKRdd.toDF("key", "value", "result")
    tag_df_ck.createOrReplaceTempView(s"${targetJsonName}_order_ck")
    val result_ck = spark.sql(
      s"""
         |select key,result from
         |(select key,value,result,row_number() over (partition by key order by value desc) rn from ${targetJsonName}_order_ck
         |) t0
         |where rn<=${topN}
      """.stripMargin)
      .rdd
      .map(_.get(1).toString)
    result_cs.union(result_ck)
  }
}
