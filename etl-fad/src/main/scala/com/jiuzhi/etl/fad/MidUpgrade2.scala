package com.jiuzhi.etl.fad

import org.apache.spark.sql.SparkSession

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil

/**
 * 关联log_upgrade和mid_version得到设备App升级信息mid_upgrade2
 */
class MidUpgrade2(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  val dbName = task.taskExt.get("hive_db").get

  def execute {
    // 读取升级下发日志log_upgrade
    spark.sql("USE " + dbName)
    val upgradeLog = spark.sql("SELECT udid, app_key, version, up_version, MIN(create_date) AS create_date FROM log_upgrade GROUP BY udid, app_key, version, up_version")

    // 读取用户使用App版本记录
    val version = spark.sql(s"SELECT udid, app_key, LAG(version) OVER(ORDER BY create_time) AS version, version AS up_version, TO_DATE(create_time) AS update_date FROM mid_version WHERE stat_date = '${task.statDate}'")
      .where("version IS NOT NULL")
    if (log.isDebugEnabled) {
      version.printSchema
      version.show(50, false)
    }

    // 关联版本
    val upgrade = upgradeLog.join(version, Seq("udid", "app_key", "version", "up_version"), "left")
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      upgrade.printSchema
      upgrade.show(50, false)
    }

    // 入库
    upgrade.createOrReplaceTempView("tmp_mid_upgrade")
    spark.sql(s"ALTER TABLE mid_upgrade2 DROP IF EXISTS PARTITION(stat_date = '${task.statDate}') PURGE")
    spark.sql(s"INSERT INTO mid_upgrade2 PARTITION(stat_date = '${task.statDate}') SELECT * FROM tmp_mid_upgrade")

    // 删除历史分区
    val preDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-2, task.theTime))
    spark.sql(s"ALTER TABLE mid_upgrade2 DROP IF EXISTS PARTITION(stat_date = '${preDate}') PURGE")
  }
}