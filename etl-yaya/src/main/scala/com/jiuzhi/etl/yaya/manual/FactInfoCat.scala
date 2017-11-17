package com.jiuzhi.etl.yaya.manual

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 资讯视频频道点击
 * 统计当天数据
 */
class FactInfoCat(task: Task) extends TaskExecutor(task) with Serializable {

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get

  // app key
  val appKey = task.taskExt.getOrElse("app_key", "jz-yaya")

  // hdfs目录
  val rootPath = task.taskExt.getOrElse("root_path", "/flume/json")
  // topic
  val topic = task.taskExt.getOrElse("topic", "event_topic_json")
  val eventDir = s"${rootPath}/${task.theDate}/${topic}"

  val statDate = task.theDate.replaceAll("-", "")

  def execute {
    // 视频资讯频道点击
    val event = spark.read.json(eventDir)
      .where(s"appkey = '${appKey}' AND event_identifier IN ('recreate_sub', 'video_sub')")

    // 视频资讯总点击
    val click1 = event.groupBy("event_identifier")
      .agg(countDistinct("deviceid").alias("total_count"))

    // 频道点击
    val click2 = event.groupBy("event_identifier", "acc")
      .agg(countDistinct("deviceid").alias("sub_count"))

    // 关联
    val result = click2.join(click1, "event_identifier")
      .withColumn("stat_date", lit(s"${statDate}"))
      .withColumnRenamed("event_identifier", "eventid")
      .coalesce(parallelism)

    // 入库
    JdbcUtil.executeUpdate(sdkDb, s"DELETE FROM fact_info_cat WHERE stat_date = ${statDate}")
    result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "fact_info_cat", sdkDb.connProps)
  }
}