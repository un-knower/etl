package com.jiuzhi.etl.yaya.manual

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 资讯/视频 点击、分享、评论
 * 统计当天数据
 */
class StatInfo(task: Task) extends TaskExecutor(task) with Serializable {

  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get

  // app key
  val appKey = task.taskExt.getOrElse("app_key", "jz-yaya")

  // hdfs目录
  val rootPath = task.taskExt.getOrElse("root_path", "/flume/json")
  // topic
  val topic = task.taskExt.getOrElse("topic", "event_topic_json")
  val eventDir = s"${rootPath}/${task.theDate}/${topic}"

  val topN = task.taskExt.getOrElse("top_count", 1).toString.toInt

  val statDate = task.theDate.replaceAll("-", "")

  def execute {
    val event = spark.read.json(eventDir)
      .where(s"appkey = '${appKey}'")

    // 点击
    val click = event.where("event_identifier IN ('recreate_click', 'video_click')")
      .groupBy("acc")
      .agg(count("*").alias("click_count"), countDistinct("deviceid").alias("click_user"))

    // 评论
    val comment = event.where("event_identifier = 'comments' AND label IN ('1', '2')")
      .groupBy("acc")
      .agg(count("*").alias("comment_count"), countDistinct("deviceid").alias("comment_user"))

    // 分享
    val share = event.where("event_identifier = 'share' AND label IN ('1', '2')")
      .groupBy("acc")
      .agg(count("*").alias("share_count"), countDistinct("deviceid").alias("share_user"))

    // 资讯
    val info = spark.read.jdbc(sdkDb.jdbcUrl, "dim_info", sdkDb.connProps)
      .select("id", "category_id", "publish_date", "create_by", "source_platform", "title", "source", "is_index", "push", "content_type")

    // 关联
    val result = click.join(comment, Seq("acc"), "left")
      .join(share, Seq("acc"), "left")
      .withColumn("stat_date", lit(s"${statDate}"))
      .withColumnRenamed("acc", "id")
      .join(info, "id")
      .withColumnRenamed("push", "is_push")
      .na.fill(Map("source_platform" -> 0))
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(sdkDb, s"DELETE FROM fact_info_detail WHERE stat_date = '${statDate}'")
    result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "fact_info_detail", sdkDb.connProps)

    // 按创建人统计点击最高的资讯
    val infoDetail = spark.read.jdbc(sdkDb.jdbcUrl, "fact_info_detail", Array(s"stat_date = ${statDate} AND publish_date = ${statDate}"), sdkDb.connProps)
      .select("content_type", "create_by", "id", "title", "publish_date", "click_count", "click_user").rdd
      .groupBy(row => row.getInt(0) + row.getString(1))
      .map {
        _._2.toSeq.sortBy(_.getInt(6)).takeRight(topN)
      }

    val sqls = infoDetail
      .coalesce(1)
      .flatMap { seq =>
        seq.map { row =>
          s"INSERT INTO top_info (id, content_type, create_by, publish_date, title, click_count, click_user) VALUES (${row.getLong(2)}, ${row.getInt(0)}, '${row.getString(1)}', ${row.getInt(4)}, '${row.getString(3)}', ${row.getInt(5)}, ${row.getInt(6)})"
        }
      }
      .collect
    JdbcUtil.executeBatch(sdkDb, sqls)
  }
}