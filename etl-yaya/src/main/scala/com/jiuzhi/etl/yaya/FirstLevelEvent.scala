package com.jiuzhi.etl.yaya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 一级事件
 */
class FirstLevelEvent(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_db_id").get.toInt).get

  // 点击事件
  val events = task.taskExt.get("events").get.split(",").map("'" + _ + "'").mkString(",")

  def execute {
    import spark.implicits._

    // 从mysql获取设备信息
    log.info("Get device information from mysql database")
    spark.read.jdbc(sdkDb.jdbcUrl, "dim_device", sdkDb.connProps).select("uuid", "customer_id").createOrReplaceTempView("tmp_device")

    // 任务重做
    if (task.redoFlag) {
      log.info("Redo task")
      val sql = s"DELETE FROM first_level_event WHERE create_date = ${task.statDate}"
      log.debug(sql)
      JdbcUtil.executeUpdate(sdkDb, sql)
    }

    val sql = "SELECT a.deviceid, b.customer_id, a.min_version, a.eventid, a.click_times " +
      s"FROM (SELECT deviceid, eventid, MIN(version) min_version, COUNT(1) click_times FROM event WHERE deviceid > '' AND version > '' AND eventid IN (${events}) AND date = '${task.prevDate}' GROUP BY deviceid, eventid) a " +
      "JOIN tmp_device b ON a.deviceid = b.uuid"
    log.debug(sql)
    log.info("Execute sql to get data and insert into table")
    spark.sql(sql)
      .coalesce(parallelism)
      .map { row =>
        (row.getString(0), row.getString(1), row.getString(2), row.getString(3), task.statDate, row.getLong(4))
      }.toDF("device_id", "customer_id", "version", "event_id", "create_date", "click_times")
      .write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "first_level_event", sdkDb.connProps)
  }
}