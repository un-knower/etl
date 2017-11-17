package com.jiuzhi.etl.yaya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.JdbcUtil

/**
 * 资讯视频详细点击
 */
class MidInfo(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get

  val days = task.taskExt.getOrElse("days", 30).toString.toInt

  def execute {
    // 点击
    val click = spark.sql(s"SELECT acc, deviceid, COUNT(1) AS click_count FROM event WHERE date = '${task.prevDate}' AND eventid IN ('recreate_click', 'video_click') GROUP BY acc, deviceid")
    if (log.isDebugEnabled) {
      click.printSchema
      click.show(50, false)
    }

    click.coalesce(parallelism).createOrReplaceTempView("tmp_mid_info")

    // 入库
    spark.sql("USE event")
    spark.sql(s"ALTER TABLE mid_info DROP IF EXISTS PARTITION(stat_date = '${task.statDate}') PURGE")
    spark.sql(s"INSERT INTO mid_info PARTITION(stat_date = '${task.statDate}') SELECT * FROM tmp_mid_info")

    // 取最近一段时间的写入fact_info
    val startDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-days, task.theTime))
    JdbcUtil.executeUpdate(sdkDb, "TRUNCATE TABLE fact_info")
    val result = spark.sql(s"SELECT * FROM mid_info WHERE stat_date >= '${startDate}' AND stat_date <= '${task.statDate}'")
    result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "fact_info", sdkDb.connProps)
  }
}