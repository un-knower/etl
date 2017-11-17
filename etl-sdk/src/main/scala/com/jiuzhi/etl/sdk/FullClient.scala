package com.jiuzhi.etl.sdk

import java.sql.SQLException
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException

import org.apache.spark.sql.SaveMode
import org.apache.spark.util.SizeEstimator

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.DateUtils
import org.zc.sched.util.JdbcUtil

import com.jiuzhi.etl.sdk.model.Client

/**
 * 客户端
 * 全量处理
 */
class FullClient(task: Task) extends TaskExecutor(task) with Serializable {

  // 日期范围
  val startDate = DateUtil.getDate(task.taskExt.getOrElse("start_date", "2017-04-18"))
  val endDate = DateUtil.getDate(task.taskExt.getOrElse("end_date", task.prevDate))

  // visit log文件目录
  val rootPath = task.taskExt.get("root_path").get
  val visitLogPaths = DateUtils.genDate(startDate, endDate).map(rootPath + _)

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get

  val factTable = "full_client"
  val bakTable = s"full_client_${task.statDate}"

  def execute {
    // 任务重做
    if (task.redoFlag) {
      JdbcUtil.executeUpdate(sdkDb, s"TRUNCATE TABLE ${factTable}")
      JdbcUtil.executeUpdate(sdkDb, s"INSERT INTO ${factTable} SELECT * FROM ${bakTable}")
    }

    // 读取hdfs json文件
    val visitlog = spark.read.json(visitLogPaths: _*)
      .where(s"createtime >= '${task.prevDate}' AND createtime < '${task.theDate}'")
      .selectExpr("uuid", "appkey", "customid", "version", "CAST(pkgpath AS INT)",
        "CAST(createtime AS TIMESTAMP) create_time", "CAST(createtime AS TIMESTAMP) update_time", "version init_version", "CAST(DATE_FORMAT(createtime, 'yyyyMMdd') AS INT)")
    if (log.isDebugEnabled) {
      visitlog.printSchema
      visitlog.show(50, false)
    }

    // 读取${factTable}
    val client = spark.read.jdbc(sdkDb.jdbcUrl, factTable, sdkDb.connProps)
      .select("uuid", "app_key", "customer_id", "version", "pkg_path",
        "create_time", "update_time", "init_version", "create_date")
    if (log.isDebugEnabled) {
      client.printSchema
      client.show(50, false)
      println("Memory consumption " + SizeEstimator.estimate(client))
    }

    import spark.implicits._

    // 合并
    val result = visitlog.union(client)
      .map(Client(_)).rdd
      .groupBy(row => row.uuid + row.app_key)
      .map {
        _._2.toSeq.sortBy(_.update_time.getTime)
          .reduceLeft { (acc, curr) => Client.update(acc, curr) }
      }
      .map(Client.finalize(_))
      .toDF()
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 写入临时表
    val tmpTable = s"tmp_${factTable}_" + System.currentTimeMillis()
    JdbcUtil.executeUpdate(sdkDb, s"CREATE TABLE ${tmpTable} LIKE ${factTable}")
    try {
      result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, tmpTable, sdkDb.connProps)
    } catch {
      case e: SQLException =>
        JdbcUtil.executeUpdate(sdkDb, s"DROP TABLE IF EXISTS ${tmpTable}")
        throw new RuntimeException(e)
    }

    // 备份${factTable}
    try {
      JdbcUtil.executeUpdate(sdkDb, s"RENAME TABLE ${factTable} TO ${bakTable}")
    } catch {
      case _: MySQLSyntaxErrorException =>
        log.info(s"Table ${bakTable} already exists")
        JdbcUtil.executeUpdate(sdkDb, s"DROP TABLE ${factTable}")
    }

    // 更新${factTable}
    JdbcUtil.executeUpdate(sdkDb, s"RENAME TABLE ${tmpTable} TO ${factTable}")

    // 删除历史数据
    val prevDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-2, task.theTime))
    JdbcUtil.executeUpdate(sdkDb, s"DROP TABLE IF EXISTS ${factTable}_${prevDate}")
  }
}