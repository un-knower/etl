package com.jiuzhi.etl.yaya

import org.apache.spark.sql.SaveMode
import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.DateUtils

/**
 * hdfs json文件导入mysql
 */
class ExportJson(task: Task) extends TaskExecutor(task) with Serializable {

  // 开始日期
  val startDate = task.taskExt.getOrElse("start_date", "2017-04-24")

  // app key
  val appKey = task.taskExt.getOrElse("app_key", "")

  // hdfs目录
  val rootPath = task.taskExt.getOrElse("root_path", "/flume/json")
  // topic
  val topic = task.taskExt.getOrElse("topic", "event_topic_json")
  val eventDirs = DateUtils.genDate(java.sql.Date.valueOf(startDate), java.sql.Date.valueOf(task.prevDate))
    .map { row =>
      val theDate = DateUtil.formatDate(row)
      s"${rootPath}/${theDate}/${topic}"
    }
  // 表名
  val tableName = topic.split("_")(0)

  // 目标数据库
  val tarDb = getDbConn(task.taskExt.get("tar_db_id").get.toInt).get

  def execute {
    // topic data
    val data = if (appKey != "") {
      spark.read.json(eventDirs: _*).where(s"appkey = '${appKey}'")
    } else {
      spark.read.json(eventDirs: _*)
    }
    data.write.mode(SaveMode.Append).jdbc(tarDb.jdbcUrl, tableName, tarDb.connProps)

    if (task.taskExt.get("focus_db_id").nonEmpty) {
      val focusDb = getDbConn(task.taskExt.get("focus_db_id").get.toInt).get

      // 粉丝团关注表
      val focus = spark.read.jdbc(focusDb.jdbcUrl, "t_fans_club_focus", Array("1 = 1 LIMIT 100000000"), focusDb.connProps)
      focus.write.mode(SaveMode.Append).jdbc(tarDb.jdbcUrl, "t_fans_club_focus", tarDb.connProps)
    }

    if (task.taskExt.get("user_db_id").nonEmpty) {
      val userDb = getDbConn(task.taskExt.get("user_db_id").get.toInt).get

      // 用户账号信息表
      val account = spark.read.jdbc(userDb.jdbcUrl, "sys_member_account", Array("1 = 1 LIMIT 100000000"), userDb.connProps)
      account.write.mode(SaveMode.Append).jdbc(tarDb.jdbcUrl, "sys_member_account", tarDb.connProps)

      // 用户登录表
      val login = spark.read.jdbc(userDb.jdbcUrl, "sys_member_reglogin", Array("1 = 1 LIMIT 100000000"), userDb.connProps)
      login.write.mode(SaveMode.Append).jdbc(tarDb.jdbcUrl, "sys_member_reglogin", tarDb.connProps)
    }
  }
}