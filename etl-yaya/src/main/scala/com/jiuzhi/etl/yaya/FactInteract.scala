package com.jiuzhi.etl.yaya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 互动点击、分享、评论
 */
class FactInteract(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // 互动数据库
  val interactDb = getDbConn(task.taskExt.get("interact_db_id").get.toInt).get
  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get

  def execute {
    // 点击
    val click = spark.sql(s"SELECT acc AS id, COUNT(1) AS click_count, COUNT(DISTINCT deviceid) AS click_user FROM event WHERE date = '${task.prevDate}' AND eventid IN ('interact_click', 'video_click') GROUP BY acc")
    if (log.isDebugEnabled) {
      click.printSchema
      click.show(50, false)
    }

    // 评论
    val comment = spark.sql(s"SELECT acc AS id, COUNT(1) AS comment_count, COUNT(DISTINCT deviceid) AS comment_user FROM event WHERE date = '${task.prevDate}' AND eventid = 'comments' AND label = '3' GROUP BY acc")
    if (log.isDebugEnabled) {
      comment.printSchema
      comment.show(50, false)
    }

    // 分享
    val share = spark.sql(s"SELECT acc AS id, COUNT(1) AS share_count, COUNT(DISTINCT deviceid) AS share_user FROM event WHERE date = '${task.prevDate}' AND eventid = 'share' AND label = '15' GROUP BY acc")
    if (log.isDebugEnabled) {
      share.printSchema
      share.show(50, false)
    }

    // 读取互动数据
    val interact = spark.read.jdbc(interactDb.jdbcUrl, "t_interact", Array(s"interact_end_time >= '${task.prevDate}' LIMIT 100000000"), interactDb.connProps)
      .selectExpr("id", "title", "CAST(DATE_FORMAT(interact_start_time, 'yyyyMMdd') AS INT) start_date", "CAST(DATE_FORMAT(interact_end_time, 'yyyyMMdd') AS INT) end_date")
    if (log.isDebugEnabled) {
      interact.printSchema
      interact.show(50, false)
    }

    // 关联
    val result = click.join(comment, Seq("id"), "left")
      .join(share, Seq("id"), "left")
      .join(interact, "id")
      .withColumn("stat_date", lit(s"${task.statDate}"))
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(sdkDb, s"DELETE FROM fact_interact WHERE stat_date = '${task.statDate}'")
    result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "fact_interact", sdkDb.connProps)
  }
}