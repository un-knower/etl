package com.jiuzhi.etl.yaya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 搜索用户占比
 */
class StatSearch(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get

  def execute {
    // 搜索
    val result = spark.sql(s"SELECT COUNT(DISTINCT deviceid) AS all_count, COUNT(DISTINCT IF(eventid = 'search', deviceid, NULL)) AS search_count FROM event WHERE date = '${task.prevDate}'")
      .coalesce(parallelism)
      .withColumn("stat_date", lit(s"${task.statDate}"))
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(sdkDb, s"DELETE FROM stat_search WHERE stat_date = ${task.statDate}")
    result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "stat_search", sdkDb.connProps)
  }
}