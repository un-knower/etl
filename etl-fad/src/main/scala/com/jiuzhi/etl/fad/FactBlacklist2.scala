package com.jiuzhi.etl.fad

import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 黑名单统计fact_blacklist2
 */
class FactBlacklist2(task: Task) extends TaskExecutor(task) with Serializable {

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

    import spark.implicits._

    // 关联
    val result = client.join(customer, "clnt")
      .map { row => (row.getString(0), row.getString(1), row.getInt(2), row.getInt(3)) }.rdd
      .groupBy(row => row._1 + " " + row._2)
      .map(row => {
        val arr = row._1.split(" ")
        val black_count = row._2.filter(row => row._3 < row._4).size
        val release_count = row._2.filter(row => row._3 == row._4 - 1).size
        (task.statDate, arr(0), arr(1), black_count, release_count)
      })
      .toDF("stat_date", "app_key", "clnt", "black_count", "release_count")
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(adDb, s"DELETE FROM fact_blacklist2 WHERE stat_date = ${task.statDate}")
    result.write.mode(SaveMode.Append).jdbc(adDb.jdbcUrl, "fact_blacklist2", adDb.connProps)
  }
}