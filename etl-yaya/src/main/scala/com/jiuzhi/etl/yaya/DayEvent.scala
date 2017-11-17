package com.jiuzhi.etl.yaya

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 点击事件
 */
class DayEvent(task: Task) extends TaskExecutor(task) with Serializable {

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
      val sql = s"DELETE FROM fact_event WHERE create_date = ${task.statDate}"
      log.debug(sql)
      JdbcUtil.executeUpdate(sdkDb, sql)
    }

    val sql = "SELECT b.customer_id, a.version, a.eventid, COUNT(1), COUNT(DISTINCT a.deviceid) " +
      s"FROM (SELECT deviceid, eventid, version FROM event WHERE date = '${task.prevDate}' AND deviceid > '' AND version > '' AND insertdate > '') a " +
      "JOIN tmp_device b ON a.deviceid = b.uuid " +
      "GROUP BY b.customer_id, a.version, a.eventid"
    log.debug(sql)
    log.info("Execute sql to get data and insert into table")
    spark.sql(sql)
      .coalesce(parallelism)
      .map { row =>
        (row.getString(0), row.getString(1), row.getString(2), task.statDate, row.getLong(3), row.getLong(4))
      }.toDF("customer_id", "version", "event_id", "create_date", "click_count", "user_count")
      .write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "fact_event", sdkDb.connProps)
  }
}