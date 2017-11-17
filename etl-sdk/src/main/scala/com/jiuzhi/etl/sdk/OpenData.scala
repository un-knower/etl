package com.jiuzhi.etl.sdk

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 开放数据
 */
class OpenData(task: Task) extends TaskExecutor(task) with Serializable {

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get
  // 客户数据库
  val cusDb = getDbConn(task.taskExt.get("cus_db_id").get.toInt).get

  def execute {
    val result = spark.read.jdbc(sdkDb.jdbcUrl, "fact_client", Array(s"create_date = '${task.statDate}'"), sdkDb.connProps)
      .groupBy("customer_id")
      .agg(count("*").alias("csum"))
      .selectExpr("customer_id AS chacode", "csum")
      .withColumn("cdate", lit(s"${task.prevDate}"))
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(cusDb, s"DELETE FROM t_channeldaily WHERE cdate = '${task.prevDate}'")
    result.write.mode(SaveMode.Append).jdbc(cusDb.jdbcUrl, "t_channeldaily", cusDb.connProps)
  }
}