package com.jiuzhi.etl.yaya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 资讯/视频 点击、分享、评论
 */
class StatInfo(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get

  def execute {
    // 点击
    val click = spark.sql(s"SELECT acc AS id, COUNT(1) AS click_count, COUNT(DISTINCT deviceid) AS click_user FROM event WHERE date = '${task.prevDate}' AND eventid IN ('recreate_click', 'video_click') GROUP BY acc")
    if (log.isDebugEnabled) {
      click.printSchema
      click.show(50, false)
    }

    // 评论
    val comment = spark.sql(s"SELECT acc AS id, COUNT(1) AS comment_count, COUNT(DISTINCT deviceid) AS comment_user FROM event WHERE date = '${task.prevDate}' AND eventid = 'comments' AND label IN ('1', '2') GROUP BY acc")
    if (log.isDebugEnabled) {
      comment.printSchema
      comment.show(50, false)
    }

    // 分享
    val share = spark.sql(s"SELECT acc AS id, COUNT(1) AS share_count, COUNT(DISTINCT deviceid) AS share_user FROM event WHERE date >= '${task.prevDate}' AND eventid = 'share' AND label IN ('1', '2') GROUP BY acc")
    if (log.isDebugEnabled) {
      share.printSchema
      share.show(50, false)
    }

    // 资讯
    val info = spark.read.jdbc(sdkDb.jdbcUrl, "dim_info", sdkDb.connProps)
      .select("id", "category_id", "publish_date", "create_by", "source_platform", "title", "source", "is_index", "push", "content_type")

    // 关联
    val result = click.join(comment, Seq("id"), "left")
      .join(share, Seq("id"), "left")
      .join(info, "id")
      .withColumn("stat_date", lit(s"${task.statDate}"))
      .withColumnRenamed("push", "is_push")
      .na.fill(Map("source_platform" -> 0))
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(sdkDb, s"DELETE FROM fact_info_detail WHERE stat_date = '${task.statDate}'")
    result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "fact_info_detail", sdkDb.connProps)
  }
}