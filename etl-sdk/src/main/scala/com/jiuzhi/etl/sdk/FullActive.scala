package com.jiuzhi.etl.sdk

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil
import org.zc.sched.util.DateUtils
import org.zc.sched.util.DateUtil

import com.jiuzhi.etl.sdk.model.Active

/**
 * 活跃
 * 全量处理
 */
class FullActive(task: Task) extends TaskExecutor(task) with Serializable {

  // 日期范围
  val startDate = DateUtil.getDate(task.taskExt.getOrElse("start_date", "2017-04-18"))
  val endDate = DateUtil.getDate(task.taskExt.getOrElse("end_date", "2017-07-05"))

  // visit log文件目录
  val rootPath = task.taskExt.get("root_path").get
  val visitLogPaths = DateUtils.genDate(startDate, endDate).map(rootPath + DateUtil.formatDate(_))

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get
  val sdkOds = getDbConn(task.taskExt.get("sdk_ods_id").get.toInt).get

  def execute {
    // 读取hdfs json文件
    val hdfslog = spark.read.json(visitLogPaths: _*)
      .selectExpr("uuid", "appkey", "DATE_FORMAT(createtime, 'yyyy-MM-dd') AS active_date", "CAST(logtype AS INT)", "version",
        "city", "region", "country", "CAST(createtime AS TIMESTAMP)")
      .where("uuid > '' AND appkey > '' AND version > ''")
    if (log.isDebugEnabled) {
      hdfslog.printSchema
      hdfslog.show(50, false)
    }

    // 读取mysql数据
    val minDate = DateUtil.getDate(task.taskExt.getOrElse("min_date", "2015-12-19"))
    val maxDate = DateUtil.getDate(task.taskExt.getOrElse("max_date", "2017-05-02"))
    val predicates = DateUtils.rangeDate(5, minDate, maxDate).map { row =>
      val startTime = DateUtil.formatDatetime(row._1)
      val endTime = DateUtil.formatDatetime(row._2)
      s"createtime >= '${startTime}' AND createtime < '${endTime}' AND uuid > '' AND appkey > '' AND version > ''"
    }
    val dblog = spark.read.jdbc(sdkOds.jdbcUrl, "ods_device_visitlog", predicates, sdkOds.connProps)
      .selectExpr("uuid", "appkey", "DATE_FORMAT(createtime, 'yyyy-MM-dd') AS active_date", "logtype", "version",
        "city", "region", "country", "createtime")
    if (log.isDebugEnabled) {
      dblog.printSchema
      dblog.show(50, false)
    }

    import spark.implicits._

    // 分析
    val active = hdfslog.union(dblog)
      .distinct
      .map(Active(_)).rdd
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
    val result = active.join(broadcast(client), Seq("uuid", "app_key"))
      .selectExpr("uuid", "app_key", "CAST(DATE_FORMAT(active_date, 'yyyyMMdd') AS INT) AS active_date", "log_type", "version",
        "city", "region", "country", "visit_times", "customer_id",
        "init_version", "create_date", "DATEDIFF(active_date, _create_date) AS date_diff")
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(sdkDb, s"RENAME TABLE fact_active TO fact_active_${endDate}")
    JdbcUtil.executeUpdate(sdkDb, s"CREATE TABLE fact_active LIKE fact_active_${endDate}")
    result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "fact_active", sdkDb.connProps)
  }
}