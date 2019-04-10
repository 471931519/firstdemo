package com.bonc.utils

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Author: YangYunhe
  * Description: 日期时间转换的工具类
  * Create: 2018-06-29 11:43
  */
object DateUtils {

  val DATE_FORMAT_STR = "yyyy-MM-dd"
  val TIME_FORMAT_STR = "HH:mm:ss"
  val DATE_TIME_FORMAT_STR = "yyyy-MM-dd HH:mm:ss"
  val ONE_DAY_TIME: java.lang.Long = 86400000L

  def parseLong2String(timestamp: java.lang.Long, pattern: String): String = {
    new SimpleDateFormat(pattern).format(new Date(timestamp))
  }

  def parseString2Long(dateTimeStr: String, pattern: String): java.lang.Long = {
    new SimpleDateFormat(pattern).parse(dateTimeStr).getTime
  }

  def nowDateStr(): String = {
    parseLong2String(now(), DATE_FORMAT_STR)
  }

  def nowTimeStr(): String = {
    parseLong2String(now(), TIME_FORMAT_STR)
  }

  def nowStr(): String = {
    parseLong2String(now(), DATE_TIME_FORMAT_STR)
  }

  def now(): java.lang.Long = {
    System.currentTimeMillis()
  }

  def oneDayAgo(): java.lang.Long = {
    now() - ONE_DAY_TIME
  }

  def daysAgo(numsDays: Int): java.lang.Long = {
    now() - (ONE_DAY_TIME * numsDays)
  }

}
