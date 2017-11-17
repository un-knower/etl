package com.jiuzhi.etl.recommend

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task

/**
 * 用户点击（浏览、评论、分享）资讯次数-日统计
 * 资讯点击人数-日统计
 */
class DayClick(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  def execute {
    // 用户点击次数
    // 删除分区
    var sql = s"ALTER TABLE d_user_click DROP IF EXISTS PARTITION(click_date = '${task.prevDate}') PURGE"
    log.debug(sql)
    spark.sql("USE recommender")
    spark.sql(sql)

    sql = s"INSERT INTO d_user_click PARTITION(click_date = '${task.prevDate}') " +
      " SELECT deviceid, acc, eventid, COUNT(1)" +
      " FROM default.event" +
      " WHERE deviceid > '' AND acc > '' AND eventid > ''" +
      s" AND date = '${task.prevDate}'" +
      " AND (eventid = 'recreate_click' OR (eventid = 'share' AND label = '1') OR (eventid = 'comments' AND label = '1'))" +
      " GROUP BY deviceid, acc, eventid"
    log.debug(sql)
    spark.sql(sql)

    // 资讯点击人数
    // 删除分区
    sql = s"ALTER TABLE d_info_click DROP IF EXISTS PARTITION(click_date = '${task.prevDate}') PURGE"
    log.debug(sql)
    spark.sql(sql)

    sql = s"INSERT INTO d_info_click PARTITION(click_date = '${task.prevDate}') " +
      " SELECT info_id, COUNT(DISTINCT device_id)" +
      " FROM d_user_click" +
      s" WHERE click_date = '${task.prevDate}'" +
      " GROUP BY info_id"
    log.debug(sql)
    spark.sql(sql)
  }
}