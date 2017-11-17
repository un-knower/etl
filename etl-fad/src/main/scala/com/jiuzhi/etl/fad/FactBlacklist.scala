package com.jiuzhi.etl.fad

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.model.Task
import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.util.JdbcUtil

/**
 * 黑名单统计fact_blacklist
 */
class FactBlacklist(task: Task) extends TaskExecutor(task) with Serializable {

  // 广告数据库
  val adDb = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get
  // 广告数据源
  val srcAdDb = getDbConn(task.taskExt.get("ad_srcdb_id").get.toInt).get

  def execute {
    // 读取fact_client
    val client = spark.read.jdbc(adDb.jdbcUrl, "fact_client", adDb.connProps)
      .selectExpr("app_key", "clnt", s"DATEDIFF('${task.prevDate}', create_time) AS date_diff")

    // 读取adv_customer
    val customer = spark.read.jdbc(srcAdDb.jdbcUrl, "adv_customer", srcAdDb.connProps)
      .selectExpr("customid AS clnt", "blackdate")

    // 关联
    val result = client.join(broadcast(customer), "clnt")
      .groupBy("app_key", "clnt")
      .agg(count(when(col("date_diff") < col("blackdate"), 1)).alias("black_count"), count(when(col("date_diff") + 1 === col("blackdate"), 1)).alias("release_count"))
      .withColumn("stat_date", lit(s"${task.statDate}"))
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(adDb, s"DELETE FROM fact_blacklist WHERE stat_date = ${task.statDate}")
    result.write.mode(SaveMode.Append).jdbc(adDb.jdbcUrl, "fact_blacklist", adDb.connProps)
  }
}