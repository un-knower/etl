package com.jiuzhi.etl.yaya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.JdbcUtil

/**
 * 互动点击、分享、评论
 */
class StatInteract(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // 互动数据库
  val interactDb = getDbConn(task.taskExt.get("interact_db_id").get.toInt).get
  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get

  // 统计上周结束互动
  val startDate = DateUtil.formatDate(DateUtil.nextDate(-7, task.theTime))
  val endDate = DateUtil.formatDate(DateUtil.nextDate(-1, task.theTime))

  def execute {
    // 获取互动最小开始时间
    val sql = s"SELECT DATE_FORMAT(MIN(create_time), '%Y-%m-%d') FROM t_interact WHERE interact_end_time >= '${startDate}' AND interact_end_time < '${endDate}' + INTERVAL 1 DAY"
    val createDate = JdbcUtil.executeQuery(interactDb, sql).toString

    // 点击
    val click = spark.sql(s"SELECT acc AS id, COUNT(1) AS click_count, COUNT(DISTINCT deviceid) AS click_user FROM event WHERE date >= '${createDate}' AND date < '${task.theDate}' AND eventid IN ('interact_click', 'video_click') GROUP BY acc")
    if (log.isDebugEnabled) {
      click.printSchema
      click.show(50, false)
    }

    // 评论
    val comment = spark.sql(s"SELECT acc AS id, COUNT(1) AS comment_count, COUNT(DISTINCT deviceid) AS comment_user FROM event WHERE date >= '${createDate}' AND date < '${task.theDate}' AND eventid = 'comments' AND label = '3' GROUP BY acc")
    if (log.isDebugEnabled) {
      comment.printSchema
      comment.show(50, false)
    }

    // 分享
    val share = spark.sql(s"SELECT acc AS id, COUNT(1) AS share_count, COUNT(DISTINCT deviceid) AS share_user FROM event WHERE date >= '${createDate}' AND date < '${task.theDate}' AND eventid = 'share' AND label = '15' GROUP BY acc")
    if (log.isDebugEnabled) {
      share.printSchema
      share.show(50, false)
    }

    // 读取互动数据
    val interact = spark.read.jdbc(interactDb.jdbcUrl, "t_interact", Array(s"interact_end_time >= '${startDate}' AND interact_end_time < '${endDate}' + INTERVAL 1 DAY LIMIT 100000000"), interactDb.connProps)
      .selectExpr("id", "title", "interact_start_time AS start_time", "interact_end_time AS end_time")
    if (log.isDebugEnabled) {
      interact.printSchema
      interact.show(50, false)
    }

    // 关联
    val dateRange = startDate.replaceAll("-", "") + "-" + endDate.replaceAll("-", "")
    val result = broadcast(interact).join(click, Seq("id"), "left")
      .join(comment, Seq("id"), "left")
      .join(share, Seq("id"), "left")
      .withColumn("date_range", lit(s"$dateRange"))
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(sdkDb, s"DELETE FROM stat_interact WHERE date_range = '$dateRange'")
    result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "stat_interact", sdkDb.connProps)
  }
}