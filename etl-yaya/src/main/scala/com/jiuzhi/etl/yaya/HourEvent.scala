package com.jiuzhi.etl.yaya

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 分时段点击事件
 */
class HourEvent(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_db_id").get.toInt).get

  def execute {
    import spark.implicits._

    // 从mysql获取设备信息
    log.info("Get device information from mysql database")
    spark.read.jdbc(sdkDb.jdbcUrl, "dim_device", sdkDb.connProps).select("uuid", "customer_id").createOrReplaceTempView("tmp_device")

    // 任务重做
    if (task.redoFlag) {
      log.info("Redo task")
      val sql = s"DELETE FROM fact_hour_event WHERE create_date = ${task.statDate}"
      log.debug(sql)
      JdbcUtil.executeUpdate(sdkDb, sql)
    }

    val sql = "SELECT b.customer_id, a.version, a.eventid, CAST(SUBSTR(a.insertdate, 12, 2) AS INT), COUNT(1), COUNT(DISTINCT a.deviceid) " +
      s"FROM (SELECT deviceid, eventid, version, insertdate FROM event WHERE date = '${task.prevDate}' AND deviceid > '' AND eventid > '' AND version > '' AND LENGTH(insertdate) = 19) a " +
      "JOIN tmp_device b ON a.deviceid = b.uuid " +
      "GROUP BY b.customer_id, a.version, a.eventid, SUBSTR(a.insertdate, 12, 2)"
    log.debug(sql)
    log.info("Execute sql to get data and insert into table")
    spark.sql(sql)
      .coalesce(parallelism)
      .map { row =>
        (row.getString(0), row.getString(1), row.getString(2), task.statDate, row.getInt(3), row.getLong(4), row.getLong(5))
      }.toDF("customer_id", "version", "event_id", "create_date", "create_hour", "click_count", "user_count")
      .write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "fact_hour_event", sdkDb.connProps)
  }
}