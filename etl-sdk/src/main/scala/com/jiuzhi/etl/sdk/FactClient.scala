package com.jiuzhi.etl.sdk

import java.sql.SQLException
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException

import org.apache.spark.sql.SaveMode
import org.apache.spark.util.SizeEstimator

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.JdbcUtil

import com.jiuzhi.etl.sdk.model.Client

/**
 * 客户端
 */
class FactClient(task: Task) extends TaskExecutor(task) with Serializable {

  // visit log文件目录
  val rootPath = task.taskExt.get("root_path").get
  val visitLogPath = rootPath + task.prevDate
  val newVisitLogPath = rootPath + task.theDate

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get

  def execute {
    // 任务重做
    if (task.redoFlag) {
      JdbcUtil.executeUpdate(sdkDb, "TRUNCATE TABLE fact_client")
      JdbcUtil.executeUpdate(sdkDb, s"INSERT INTO fact_client SELECT * FROM fact_client_${task.statDate}")
    }

    // 读取hdfs json文件
    val visitlog = spark.read.json(visitLogPath, newVisitLogPath)
      .where(s"createtime >= '${task.prevDate}' AND createtime < '${task.theDate}'")
      .selectExpr("uuid", "appkey", "customid", "version", "CAST(pkgpath AS INT)",
        "CAST(createtime AS TIMESTAMP) create_time", "CAST(createtime AS TIMESTAMP) update_time", "version init_version", "CAST(DATE_FORMAT(createtime, 'yyyyMMdd') AS INT)")
    if (log.isDebugEnabled) {
      visitlog.printSchema
      visitlog.show(50, false)
    }

    // 读取fact_client
    val client = spark.read.jdbc(sdkDb.jdbcUrl, "fact_client", sdkDb.connProps)
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
    val tmpTable = "tmp_fact_client_" + System.currentTimeMillis()
    JdbcUtil.executeUpdate(sdkDb, s"CREATE TABLE ${tmpTable} LIKE fact_client")
    try {
      result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, tmpTable, sdkDb.connProps)
    } catch {
      case e: SQLException =>
        JdbcUtil.executeUpdate(sdkDb, s"DROP TABLE IF EXISTS ${tmpTable}")
        throw new RuntimeException(e)
    }

    // 备份fact_client
    try {
      JdbcUtil.executeUpdate(sdkDb, s"RENAME TABLE fact_client TO fact_client_${task.statDate}")
    } catch {
      case _: MySQLSyntaxErrorException =>
        log.info(s"Table fact_client_${task.statDate} already exists")
        JdbcUtil.executeUpdate(sdkDb, "DROP TABLE fact_client")
    }

    // 更新fact_client
    JdbcUtil.executeUpdate(sdkDb, s"RENAME TABLE ${tmpTable} TO fact_client")

    // 删除历史数据
    val prevDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-3, task.theTime))
    JdbcUtil.executeUpdate(sdkDb, s"DROP TABLE IF EXISTS fact_client_${prevDate}")

    // 更新版本维度表dim_version
    val version = visitlog.select("version")
      .where("version > ''")
      .distinct
      .coalesce(1)
    val sqls = version.map { row => s"INSERT IGNORE INTO dim_version (id) VALUES ('${row.getString(0)}')" }
    JdbcUtil.executeBatch(sdkDb, sqls.collect)
  }
}