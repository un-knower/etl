package com.jiuzhi.etl.fad

import org.apache.spark.sql.SparkSession

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task

/**
 * 解析升级下发日志得到log_upgrade
 */
class LogUpgrade(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // upgrade log文件目录
  val rootPath = task.taskExt.get("root_path").get
  val topic = task.taskExt.get("topic").get
  val upgradeLogPath = rootPath + task.prevDate + "/" + topic
  val newUpgradeLogPath = rootPath + task.theDate + "/" + topic

  // 精确模式
  val strict = task.taskExt.getOrElse("is_strict", 1).toString.toInt

  val dbName = task.taskExt.get("hive_db").get

  def execute {
    // 读取hdfs json文件
    val upgrade = if (task.isFirst) {
      // 初始化从MySQL数据库读
      // 广告数据库
      val adDb = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get
      spark.read.jdbc(adDb.jdbcUrl, "t_upgrade_logs", Array(s"createTime < '${task.theDate}'"), adDb.connProps)
        .selectExpr("udid", "apppkg", "appVersion", "upVersion", "TO_DATE(createtime)")
    } else {
      if (strict == 1) {
        spark.read.option("allowUnquotedFieldNames", true).json(upgradeLogPath, newUpgradeLogPath)
          .where(s"createtime >= '${task.prevDate}' AND createtime < '${task.theDate}' AND udid > ''")
          .selectExpr("udid", "apppkg", "appversion", "upversion", "TO_DATE(createtime)")
      } else {
        spark.read.option("allowUnquotedFieldNames", true).json(upgradeLogPath)
          .where(s"createtime >= '${task.prevDate}' AND createtime < '${task.theDate}' AND udid > ''")
          .selectExpr("udid", "apppkg", "appversion", "upversion", "TO_DATE(createtime)")
      }
    }
    if (log.isErrorEnabled) {
      upgrade.printSchema
      upgrade.show(50, false)
    }

    // 入库
    upgrade.distinct
      .coalesce(parallelism)
      .createOrReplaceTempView("tmp_log_upgrade")
    spark.sql("USE " + dbName)
    spark.sql(s"ALTER TABLE log_upgrade DROP IF EXISTS PARTITION(stat_date = '${task.statDate}') PURGE")
    spark.sql(s"INSERT INTO log_upgrade PARTITION(stat_date = '${task.statDate}') SELECT * FROM tmp_log_upgrade")
  }
}