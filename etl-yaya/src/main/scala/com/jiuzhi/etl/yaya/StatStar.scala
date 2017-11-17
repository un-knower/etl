package com.jiuzhi.etl.yaya

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 明星访问、粉丝、动态、帖子、评论
 */
class StatStar(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // 明星数据库
  val starDb = getDbConn(task.taskExt.get("star_db_id").get.toInt).get
  // 基线数据库
  val sdkDb = getDbConn(task.taskExt.get("sdk_ndb_id").get.toInt).get

  def execute {
    // 主页点击
    val click = spark.sql(s"SELECT CAST(acc AS BIGINT) AS star_id, COUNT(1) AS click_count, COUNT(DISTINCT deviceid) AS click_user FROM event WHERE date = '${task.prevDate}' AND eventid = 'starinfo' GROUP BY acc")
    if (log.isDebugEnabled) {
      click.printSchema
      click.show(50, false)
    }

    import spark.implicits._

    // 新增粉丝
    // 注意: where条件只能通过predicates参数传递
    val fans = spark.read.jdbc(starDb.jdbcUrl, "t_fan_focus_judge", Array(s"create_time >= '${task.prevDate}' AND create_time < '${task.theDate}' LIMIT 100000000"), starDb.connProps)
      .select("star_id")
      .map(row => (row.getLong(0), 1)).rdd
      .reduceByKey(_ + _)
      .toDF("star_id", "fans_count")
    if (log.isDebugEnabled) {
      fans.printSchema
      fans.show(50, false)
    }

    // 新增动态
    val dynamic = spark.read.jdbc(starDb.jdbcUrl, "t_star_today_dynamic_sns", Array(s"dynamic_sns_date = '${task.prevDate}' LIMIT 100000000"), starDb.connProps)
      .select("star_id", "dynamic_num")
    if (log.isDebugEnabled) {
      dynamic.printSchema
      dynamic.show(50, false)
    }

    // 新增帖子
    val post = spark.read.jdbc(starDb.jdbcUrl, "t_fan_posts_star", Array(s"publish_time >= '${task.prevDate}' AND publish_time < '${task.theDate}' AND star_id > 0 LIMIT 100000000"), starDb.connProps)
      .select("star_id")
      .map(row => (row.getLong(0), 1)).rdd
      .reduceByKey(_ + _)
      .toDF("star_id", "post_count")
    if (log.isDebugEnabled) {
      post.printSchema
      post.show(50, false)
    }

    // 新增评论
    val comment = spark.read.jdbc(starDb.jdbcUrl, "t_fan_comment_today_sns", Array(s"sns_date = '${task.prevDate}' LIMIT 100000000"), starDb.connProps)
      .select("star_id")
      .map(row => (row.getLong(0), 1)).rdd
      .reduceByKey(_ + _)
      .toDF("star_id", "comment_count")
    if (log.isDebugEnabled) {
      comment.printSchema
      comment.show(50, false)
    }

    // 获取明星姓名
    val stars = spark.read.jdbc(starDb.jdbcUrl, "t_interact_star", Array("1 = 1 LIMIT 100000000"), starDb.connProps)
      .selectExpr("id AS star_id", "real_name")
    if (log.isDebugEnabled) {
      stars.printSchema
      stars.show(50, false)
    }

    // 关联
    // 注意: join字段的数据类型一定要相同
    val result = stars.join(click, Seq("star_id"), "left")
      .join(fans, Seq("star_id"), "left")
      .join(dynamic, Seq("star_id"), "left")
      .join(post, Seq("star_id"), "left")
      .join(comment, Seq("star_id"), "left")
      .withColumn("create_date", lit(s"${task.statDate}"))
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }

    // 入库
    JdbcUtil.executeUpdate(sdkDb, s"DELETE FROM stat_star WHERE create_date = '${task.statDate}'")
    result.write.mode(SaveMode.Append).jdbc(sdkDb.jdbcUrl, "stat_star", sdkDb.connProps)
  }
}