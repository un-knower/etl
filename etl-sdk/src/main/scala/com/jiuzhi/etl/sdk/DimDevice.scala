package com.jiuzhi.etl.sdk

import java.sql.SQLException
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException

import org.apache.spark.sql.SaveMode
import org.apache.spark.util.SizeEstimator

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.DateUtils
import org.zc.sched.util.JdbcUtil

import com.jiuzhi.etl.sdk.model.Device

/**
 * 设备
 */
class DimDevice(task: Task) extends TaskExecutor(task) with Serializable {

  // visit log文件目录
  val rootPath = task.taskExt.get("root_path").get
  val visitLogPath = rootPath + task.prevDate
  val newVisitLogPath = rootPath + task.theDate

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get

  val startDate = DateUtil.getDate(task.taskExt.getOrElse("start_date", "2015-12-19").toString)
  val partitionDay = task.taskExt.getOrElse("partition_day", 30).toString.toInt

  def execute {
    // 任务重做
    if (task.redoFlag) {
      JdbcUtil.executeUpdate(sdkDb, "TRUNCATE TABLE dim_device")
      JdbcUtil.executeUpdate(sdkDb, s"INSERT INTO dim_device SELECT * FROM dim_device_${task.statDate}")
    }

    // 读取hdfs json文件
    val visitlog = spark.read.json(visitLogPath, newVisitLogPath)
      .where(s"createtime >= '${task.prevDate}' AND createtime < '${task.theDate}'")
      .selectExpr("uuid", "deviceid", "appkey", "customid",
        "CASE WHEN network IN ('2G', '3G', '4G') THEN network WHEN network = 'WIFI' THEN '1WIFI' ELSE '0Unknown' END",
        "platform", "IF(havaapn = '1', 1, 0)", "imsi", "wifimac", "imei",
        "androidid", "baseband", "language", "resolution", "modulename",
        "cpu", "devicename", "osVersion", "cameras", "exttotalsize",
        "romtotalsize", "phoneno", "city", "region", "country",
        "CAST(uidtype AS INT)", "0", "CAST(createtime AS TIMESTAMP) create_time", "CAST(createtime AS TIMESTAMP) update_time")
    if (log.isDebugEnabled) {
      visitlog.printSchema
      visitlog.show(50, false)
    }

    // 读取dim_device
    val predicates = DateUtils.rangeDate(partitionDay, startDate).map { row =>
      val startTime = DateUtil.formatDatetime(row._1)
      val endTime = DateUtil.formatDatetime(row._2)
      s"create_time >= '${startTime}' AND create_time < '${endTime}'"
    }
    val device = spark.read.jdbc(sdkDb.jdbcUrl, "dim_device", predicates, sdkDb.connProps)
      .selectExpr("uuid", "device_id", "app_key", "customer_id",
        "CASE WHEN network = 'WIFI' THEN '1WIFI' WHEN network = 'Unknown' THEN '0Unknown' ELSE network END",
        "platform", "have_vpn", "imsi", "wifi_mac", "imei",
        "android_id", "baseband", "language", "resolution", "model_name",
        "cpu", "device_name", "os_version", "cameras", "sdcard_size",
        "rom_size", "phone_no", "city", "region", "country",
        "uuid_type", "isp_code", "create_time", "update_time")
    if (log.isDebugEnabled) {
      device.printSchema
      device.show(50, false)
      println("Memory consumption " + SizeEstimator.estimate(device))
    }

    import spark.implicits._

    // 合并
    val result = visitlog.union(device)
      .map(Device(_)).rdd
      .groupBy(_.uuid)
      .map {
        _._2.toSeq.sortBy(_.update_time.getTime)
          .reduceLeft { (acc, curr) => Device.update(acc, curr) }
      }
      .map(Device.finalize(_))
      .toDF()
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 写入临时表
    val tmpTable = "tmp_dim_device_" + System.currentTimeMillis()
    JdbcUtil.executeUpdate(sdkDb, s"CREATE TABLE ${tmpTable} LIKE dim_device")
    try {
      result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, tmpTable, sdkDb.connProps)
    } catch {
      case e: SQLException =>
        JdbcUtil.executeUpdate(sdkDb, s"DROP TABLE IF EXISTS ${tmpTable}")
        throw new RuntimeException(e)
    }

    // 备份dim_device
    try {
      JdbcUtil.executeUpdate(sdkDb, s"RENAME TABLE dim_device TO dim_device_${task.statDate}")
    } catch {
      case _: MySQLSyntaxErrorException =>
        log.info(s"Table dim_device_${task.statDate} already exists")
        JdbcUtil.executeUpdate(sdkDb, "DROP TABLE dim_device")
    }

    // 更新dim_device
    JdbcUtil.executeUpdate(sdkDb, s"RENAME TABLE ${tmpTable} TO dim_device")

    // 删除历史数据
    val prevDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-3, task.theTime))
    JdbcUtil.executeUpdate(sdkDb, s"DROP TABLE IF EXISTS dim_device_${prevDate}")
  }
}