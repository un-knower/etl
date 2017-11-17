package com.jiuzhi.etl.fad

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.JdbcUtil

/**
 * 取一段时间的mid_upgrade得到fact_upgrade
 */
class FactUpgrade(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // 广告数据库
  val adDb = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get

  // 天数
  val days = task.taskExt.getOrElse("days", 60).toString.toInt
  val startDate = DateUtil.formatDate(DateUtil.nextDate(-days))

  val dbName = task.taskExt.get("hive_db").get

  def execute {
    // 读取mid_upgrade
    val sql = s"SELECT udid, app_key, version, up_version, create_date, upgrade_date" +
      " FROM mid_upgrade" +
      s" WHERE stat_date = '${task.statDate}'" +
      s" AND create_date >= '${startDate}'"
    spark.sql("USE " + dbName)
    val upgrade = spark.sql(sql)
    if (log.isDebugEnabled) {
      upgrade.printSchema
      upgrade.show(50, false)
    }

    // 读取fact_client
    val client = spark.read.jdbc(adDb.jdbcUrl, "fact_client", adDb.connProps)
      .select("udid", "app_key", "clnt")
    if (log.isDebugEnabled) {
      client.printSchema
      client.show(50, false)
    }

    // 关联得到clnt
    val result = upgrade.join(client, Seq("udid", "app_key"))
      .selectExpr("udid", "app_key", "version", "up_version", "DATE_FORMAT(create_date, 'yyyyMMdd') AS create_date", "DATE_FORMAT(upgrade_date, 'yyyyMMdd') AS upgrade_date", "clnt")
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(adDb, "TRUNCATE TABLE fact_upgrade")
    result.write.mode(SaveMode.Append).jdbc(adDb.jdbcUrl, "fact_upgrade", adDb.connProps)
  }
}