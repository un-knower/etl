package com.jiuzhi.etl.fad

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.AnalysisException

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil

/**
 * 解析广告下发反馈日志得到广告下发反馈信息mid_ad
 */
class MidAd(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // ad request log文件目录
  val rootPath = task.taskExt.get("root_path").get
  val requestTopic = task.taskExt.get("request_topic").get
  val requestLogPath = rootPath + task.prevDate + "/" + requestTopic
  val newRequestLogPath = rootPath + task.theDate + "/" + requestTopic

  // ad reply log文件目录
  val replyTopic = task.taskExt.get("reply_topic").get
  val replyLogPath = rootPath + task.prevDate + "/" + replyTopic
  val newReplyLogPath = rootPath + task.theDate + "/" + replyTopic

  // 精确模式
  val strict = task.taskExt.getOrElse("is_strict", 1).toString.toInt

  // 广告数据库
  val adDb = getDbConn(task.taskExt.get("ad_db_id").get.toInt).get

  val dbName = task.taskExt.get("hive_db").get

  def execute {
    // 读取hdfs json下发日志
    val requestLog = if (task.isFirst) {
      // 初始化从MySQL数据库读
      spark.read.jdbc(adDb.jdbcUrl, "t_req_ad", Array(s"createtime < '${task.theDate}'"), adDb.connProps)
        .selectExpr("adcode", "udid", "adkey", "0 adverid", "position", "apppkg", "clnt", "cityId", "createtime")
    } else {
      if (strict == 1) {
        spark.read.option("allowUnquotedFieldNames", true).json(requestLogPath, newRequestLogPath)
          .where(s"createtime >= '${task.prevDate}' AND createtime < '${task.theDate}' AND udid > ''")
          .select("adcode", "udid", "adkey", "adverid", "position", "apppkg", "clnt", "cityid", "createtime")
      } else {
        try {
          spark.read.option("allowUnquotedFieldNames", true).json(requestLogPath)
            .select("adcode", "udid", "adkey", "adverid", "position", "apppkg", "clnt", "cityid", "createtime")
        } catch {
          case e: AnalysisException if (e.getMessage().contains("cannot resolve")) => spark.read.option("allowUnquotedFieldNames", true).json(requestLogPath)
            .selectExpr("adcode", "udid", "adkey", "0 adverid", "position", "apppkg", "clnt", "cityid", "createtime")
          case e: AnalysisException if (e.getMessage().contains("Path does not exist")) => spark.emptyDataFrame
        }
      }
    }
    if (log.isDebugEnabled) {
      requestLog.printSchema
      requestLog.show(50, false)
    }

    // 联合mid_ad得到全量下发日志
    val prevDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-2, task.theTime))
    spark.sql("USE " + dbName)
    val send = if (requestLog.columns.isEmpty) {
      spark.sql(s"SELECT adcode, udid, adkey, adverid, position, app_key, clnt, city_id, send_time FROM mid_ad WHERE stat_date = '${prevDate}'")
    } else {
      spark.sql(s"SELECT adcode, udid, adkey, adverid, position, app_key, clnt, city_id, send_time FROM mid_ad WHERE stat_date = '${prevDate}'")
        .union(requestLog)
    }
      .dropDuplicates(Seq("adcode", "udid"))
    if (log.isDebugEnabled) {
      send.printSchema
      send.show(50, false)
    }

    // 读取hdfs json反馈日志
    val replyLog = if (task.isFirst) {
      spark.read.jdbc(adDb.jdbcUrl, "t_channel_stat", Array(s"createtime < '${task.theDate}'"), adDb.connProps)
        .select("adcode", "udid", "createtime", "rtype")
    } else {
      if (strict == 1) {
        spark.read.option("allowUnquotedFieldNames", true).json(replyLogPath, newReplyLogPath)
          .where(s"createtime >= '${task.prevDate}' AND createtime < '${task.theDate}' AND udid > ''")
          .select("adcode", "udid", "createtime", "rtype")
      } else {
        spark.read.option("allowUnquotedFieldNames", true).json(replyLogPath)
          .select("adcode", "udid", "createtime", "rtype")
      }
    }
    if (log.isDebugEnabled) {
      replyLog.printSchema
      replyLog.show(50, false)
    }

    // 联合mid_ad得到全量展现日志
    val show = spark.sql(s"SELECT adcode, udid, show_time FROM mid_ad WHERE stat_date = '${prevDate}' AND show_time IS NOT NULL")
      .union(replyLog.where("rtype = 1").drop("rtype"))
      .dropDuplicates(Seq("adcode", "udid"))
    if (log.isDebugEnabled) {
      show.printSchema
      show.show(50, false)
    }

    // 联合mid_ad得到全量点击点击
    val click = spark.sql(s"SELECT adcode, udid, click_time FROM mid_ad WHERE stat_date = '${prevDate}' AND click_time IS NOT NULL")
      .union(replyLog.where("rtype = 2").drop("rtype"))
      .dropDuplicates(Seq("adcode", "udid"))
    if (log.isDebugEnabled) {
      click.printSchema
      click.show(50, false)
    }

    // 联合mid_ad得到全量安装日志
    val install = spark.sql(s"SELECT adcode, udid, install_time FROM mid_ad WHERE stat_date = '${prevDate}' AND install_time IS NOT NULL")
      .union(replyLog.where("rtype = 3").drop("rtype"))
      .dropDuplicates(Seq("adcode", "udid"))
    if (log.isDebugEnabled) {
      install.printSchema
      install.show(50, false)
    }

    // 联合mid_ad得到全量关闭日志
    val close = spark.sql(s"SELECT adcode, udid, close_time FROM mid_ad WHERE stat_date = '${prevDate}' AND close_time IS NOT NULL")
      .union(replyLog.where("rtype = 4").drop("rtype"))
      .dropDuplicates(Seq("adcode", "udid"))
    if (log.isDebugEnabled) {
      close.printSchema
      close.show(50, false)
    }

    // 联合mid_ad得到全量卸载日志
    val uninstall = spark.sql(s"SELECT adcode, udid, uninstall_time FROM mid_ad WHERE stat_date = '${prevDate}' AND uninstall_time IS NOT NULL")
      .union(replyLog.where("rtype = 5").drop("rtype"))
      .dropDuplicates(Seq("adcode", "udid"))
    if (log.isDebugEnabled) {
      uninstall.printSchema
      uninstall.show(50, false)
    }

    // 关联
    val result = send.join(show, Seq("adcode", "udid"), "left")
      .join(click, Seq("adcode", "udid"), "left")
      .join(install, Seq("adcode", "udid"), "left")
      .join(close, Seq("adcode", "udid"), "left")
      .join(uninstall, Seq("adcode", "udid"), "left")
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    result.createOrReplaceTempView("tmp_mid_ad")
    spark.sql(s"ALTER TABLE mid_ad DROP IF EXISTS PARTITION(stat_date = '${task.statDate}') PURGE")
    spark.sql(s"INSERT INTO mid_ad PARTITION(stat_date = '${task.statDate}') SELECT * FROM tmp_mid_ad")

    // 删除历史分区
    val oldDate = DateUtil.formatDate("yyyyMMdd", DateUtil.nextDate(-4, task.theTime))
    spark.sql(s"ALTER TABLE mid_ad DROP IF EXISTS PARTITION(stat_date = '${oldDate}') PURGE")
  }
}