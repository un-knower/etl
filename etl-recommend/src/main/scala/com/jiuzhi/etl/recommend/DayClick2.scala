package com.jiuzhi.etl.recommend

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil

/**
 * 用户点击（浏览、评论、分享）资讯次数-日统计
 * 资讯点击人数-日统计
 */
class DayClick2(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // 用户数据分区个数
  val userPartitionNum = task.taskExt.getOrElse("user_partition_num", 1).toString().toInt
  // 资讯数据分区个数
  val infoPartitionNum = task.taskExt.getOrElse("info_partition_num", 1).toString().toInt

  def execute {
    // 用户点击次数
    // 删除分区
    val partDate = DateUtil.formatDate(task.prevTime)
    var sql = s"ALTER TABLE d_user_click DROP IF EXISTS PARTITION(click_date = '${partDate}') PURGE"
    log.debug(sql)
    spark.sql("USE recommender")
    spark.sql(sql)

    sql = "SELECT deviceid, acc, eventid, COUNT(1) FROM default.event" +
      " WHERE deviceid > '' AND acc > '' AND eventid > ''" +
      s" AND date = '${partDate}'" +
      " AND (eventid = 'recreate_click' OR (eventid = 'share' AND label = '1') OR (eventid = 'comments' AND label = '1'))" +
      " GROUP BY deviceid, acc, eventid"
    log.debug(sql)
    spark.sql(sql).coalesce(userPartitionNum).createOrReplaceTempView("tmp_user_click")
    spark.sql(s"INSERT INTO d_user_click PARTITION(click_date = '${partDate}') SELECT * FROM tmp_user_click")

    // 资讯点击人数
    // 删除分区
    sql = s"ALTER TABLE d_info_click DROP IF EXISTS PARTITION(click_date = '${partDate}') PURGE"
    log.debug(sql)
    spark.sql(sql)

    sql = s"SELECT info_id, COUNT(DISTINCT device_id) FROM d_user_click WHERE click_date = '${partDate}' GROUP BY info_id"
    log.debug(sql)
    spark.sql(sql).coalesce(infoPartitionNum).createOrReplaceTempView("tmp_info_click")
    spark.sql(s"INSERT INTO d_info_click PARTITION(click_date = '${partDate}') SELECT * FROM tmp_info_click")
  }
}