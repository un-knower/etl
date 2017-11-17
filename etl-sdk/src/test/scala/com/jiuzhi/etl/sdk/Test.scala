package com.jiuzhi.etl.sdk

import org.zc.sched.util.DateUtils
import org.zc.sched.util.DateUtil

object Test {

  def range_date {
    DateUtils.rangeDate(30, DateUtil.getDate("2015-12-19")).map { row =>
      val startTime = DateUtil.formatDatetime(row._1)
      val endTime = DateUtil.formatDatetime(row._2)
      println(s"create_time >= '${startTime}' AND create_time < '${endTime}'")
    }
  }

  def main(args: Array[String]): Unit = {
    range_date
  }

}