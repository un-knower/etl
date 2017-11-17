package com.jiuzhi.etl.yaya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 资讯视频频道点击
 */
class FactInfoCat(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get

  def execute {
    // 视频资讯总点击
    val click1 = spark.sql(s"SELECT eventid, COUNT(DISTINCT deviceid) total_count FROM event WHERE date = '${task.prevDate}' AND eventid IN ('recreate_sub', 'video_sub') GROUP BY eventid")
    if (log.isDebugEnabled) {
      click1.printSchema
      click1.show(50, false)
    }

    // 频道点击
    val click2 = spark.sql(s"SELECT eventid, acc, COUNT(DISTINCT deviceid) sub_count FROM event WHERE date = '${task.prevDate}' AND eventid IN ('recreate_sub', 'video_sub') GROUP BY eventid, acc")
    if (log.isDebugEnabled) {
      click2.printSchema
      click2.show(50, false)
    }

    // 关联
    val result = click2.join(click1, "eventid")
      .withColumn("stat_date", lit(s"${task.statDate}"))

    // 入库
    JdbcUtil.executeUpdate(sdkDb, s"DELETE FROM fact_info_cat WHERE stat_date = ${task.statDate}")
    result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "fact_info_cat", sdkDb.connProps)
  }
}