package com.jiuzhi.etl.sdk

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

import com.jiuzhi.etl.sdk.model.Active

/**
 * 活跃
 */
class FactActive(task: Task) extends TaskExecutor(task) with Serializable {

  // visit log文件目录
  val rootPath = task.taskExt.get("root_path").get
  val visitLogPath = rootPath + task.prevDate
  val newVisitLogPath = rootPath + task.theDate

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get

  def execute {
    // 读取hdfs json文件
    val visitlog = spark.read.json(visitLogPath, newVisitLogPath)
      .where(s"createtime >= '${task.prevDate}' AND createtime < '${task.theDate}'")
      .selectExpr("uuid", "appkey", "DATE_FORMAT(createtime, 'yyyy-MM-dd') AS active_date", "CAST(logtype AS INT)", "version",
        "city", "region", "country", "CAST(createtime AS TIMESTAMP)")
    if (log.isDebugEnabled) {
      visitlog.printSchema
      visitlog.show(50, false)
    }

    import spark.implicits._

    // 分析
    val active = visitlog.map(Active(_)).rdd
      .groupBy(row => row.uuid + row.app_key + row.active_date + row.log_type)
      .map {
        _._2.toSeq.sortBy(_.create_time.getTime)
          .reduceLeft { (acc, curr) => Active.update(acc, curr) }
      }
      .toDF()
      .drop("create_time")
    if (log.isDebugEnabled) {
      active.printSchema
      active.show(50, false)
    }

    // 读取fact_client
    val client = spark.read.jdbc(sdkDb.jdbcUrl, "fact_client", sdkDb.connProps)
      .selectExpr("uuid", "app_key", "customer_id", "init_version", "create_date", "DATE_FORMAT(create_time, 'yyyy-MM-dd') AS _create_date")
    if (log.isDebugEnabled) {
      client.printSchema
      client.show(50, false)
    }

    // 关联fact_client获取init_version customer_id create_date等信息
    val result = broadcast(active).join(client, Seq("uuid", "app_key"))
      .selectExpr("uuid", "app_key", "CAST(DATE_FORMAT(active_date, 'yyyyMMdd') AS INT) AS active_date", "log_type", "version",
        "city", "region", "country", "visit_times", "customer_id",
        "init_version", "create_date", "DATEDIFF(active_date, _create_date) AS date_diff")
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(sdkDb, s"DELETE FROM fact_active WHERE active_date = ${task.statDate}")
    result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "fact_active", sdkDb.connProps)
  }
}