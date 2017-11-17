package com.jiuzhi.etl.yaya.manual

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 资讯视频详细点击
 * 统计当天数据
 */
class FactInfo(task: Task) extends TaskExecutor(task) with Serializable {

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
    import spark.implicits._

    // 视频资讯点击
    val result = spark.read.json(eventDir)
      .where(s"appkey = '${appKey}' AND event_identifier IN ('recreate_sub', 'video_sub')")
      .groupBy("acc", "deviceid")
      .agg(count("*").alias("click_count"))
      .withColumnRenamed("acc", "id")
      .withColumn("stat_date", lit(s"${statDate}"))
      .coalesce(parallelism)

    // 入库
    JdbcUtil.executeUpdate(sdkDb, s"DELETE FROM fact_info WHERE stat_date = ${statDate}")
    result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "fact_info", sdkDb.connProps)
  }
}