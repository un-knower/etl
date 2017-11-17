package com.jiuzhi.etl.fad

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.JdbcUtil

/**
 * 取一段时间的mid_ad得到fact_ad
 */
class FactAd(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // 广告数据库
  val adDb = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get

  // 天数
  val days = task.taskExt.getOrElse("days", 30).toString.toInt
  val startDate = DateUtil.formatDate(DateUtil.nextDate(-days))

  val dbName = task.taskExt.get("hive_db").get

  def execute {
    // 读取mid_ad
    spark.sql("USE " + dbName)
    val sql = "SELECT adcode, udid, adkey, adverid, position, app_key, clnt, city_id, " +
      "DATE_FORMAT(send_time, 'yyyyMMdd') AS send_date, HOUR(send_time) AS send_hour, " +
      "DATE_FORMAT(show_time, 'yyyyMMdd') AS show_date, HOUR(show_time) AS show_hour, " +
      "DATE_FORMAT(click_time, 'yyyyMMdd') AS click_date, HOUR(click_time) AS click_hour, " +
      "DATE_FORMAT(install_time, 'yyyyMMdd') AS install_date, HOUR(install_time) AS install_hour, " +
      "DATE_FORMAT(close_time, 'yyyyMMdd') AS close_date, HOUR(close_time) AS close_hour, " +
      "DATE_FORMAT(uninstall_time, 'yyyyMMdd') AS uninstall_date, HOUR(uninstall_time) AS uninstall_hour" +
      " FROM mid_ad" +
      s" WHERE stat_date = '${task.statDate}'" +
      s" AND send_time >= '${startDate}'"
    val result = spark.sql(sql)
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(adDb, "TRUNCATE TABLE fact_ad")
    result.write.mode(SaveMode.Append).jdbc(adDb.jdbcUrl, "fact_ad", adDb.connProps)
  }
}