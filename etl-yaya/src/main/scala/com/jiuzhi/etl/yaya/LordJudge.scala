package com.jiuzhi.etl.yaya

import org.apache.spark.sql.SparkSession
import org.apache.phoenix.spark._

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

/**
 * 粉丝申请圈主判断
 */
class LordJudge(task: Task) extends TaskExecutor(task) with Serializable {

  override lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  val zkUrl = task.taskExt.get("zk_url").get

  // 明星数据库
  val starDb = getDbConn(task.taskExt.get("star_db_id").get.toInt).get

  def execute {
    // 粉丝最近关注此明星时间
    val focus = spark.sqlContext.phoenixTableAsDataFrame("FOCUS",
      Seq("STARID", "ACCID", "UPDATETIME"),
      predicate = Some(s"STARID != 'null' AND ACCID != 'null' AND UPDATETIME >= '${task.prevDate}' AND UPDATETIME < '${task.theDate}'"),
      zkUrl = Some(zkUrl))
    if (log.isDebugEnabled) {
      focus.printSchema
      focus.show(50, false)
    }

    // 更新关注
    focus.coalesce(parallelism)
      .foreachPartition { iterator =>
        val sqls = iterator.map { row =>
          s"INSERT INTO t_fan_apply_header_filter_record (star_id, user_id, focus_time) VALUES (${row.getString(0)}, ${row.getString(1)}, '${row.getString(2)}') ON DUPLICATE KEY UPDATE focus_time = '${row.getString(2)}'"
        }
        if (sqls.nonEmpty) JdbcUtil.executeBatch(starDb, sqls.toArray)
      }

    // 粉丝动态数 粉丝动态置顶数 粉丝动态加精数
    val dynamic = spark.sql(s"SELECT starId, userId, dnum, topnum, creamnum FROM fans_dynamic_info WHERE starId != 'null' AND userId != 'null' AND date = '${task.prevDate}'")
    if (log.isDebugEnabled) {
      dynamic.printSchema
      dynamic.show(50, false)
    }

    // 更新动态
    dynamic.coalesce(parallelism)
      .foreachPartition { iterator =>
        val sqls = iterator.map { row =>
          s"INSERT INTO t_fan_apply_header_filter_record (star_id, user_id, fan_dynamic_num, fan_dynamic_top_num, fan_dynamic_cream_num) VALUES (${row.getString(0)}, ${row.getString(1)}, ${row.getLong(2)}, ${row.getLong(3)}, ${row.getLong(4)}) ON DUPLICATE KEY UPDATE fan_dynamic_num = ${row.getLong(2)}, fan_dynamic_top_num = ${row.getLong(3)}, fan_dynamic_cream_num = ${row.getLong(4)}"
        }
        if (sqls.nonEmpty) JdbcUtil.executeBatch(starDb, sqls.toArray)
      }

    import spark.implicits._

    // 粉丝月排行榜排在1 2 3名的次数
    val rank = spark.read.jdbc(starDb.jdbcUrl, "t_fan_month_index_rand", Array("1 = 1 LIMIT 100000000"), starDb.connProps)
      .select("star_id", "user_id", "year", "month", "total_index", "update_time")
      .map { row => (row.getLong(0), row.getLong(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getTimestamp(5)) }.rdd
      .groupBy(row => row._1 + "-" + row._3 + row._4)
      .flatMap {
        _._2.toSeq.sortWith { (bigger, smaller) =>
          if (bigger._5 == smaller._5)
            bigger._6.getTime < smaller._6.getTime
          else bigger._5 > smaller._5
        }.take(3)
          .map(row => (row._1, row._2))
      }
      .groupBy(row => row._1 + "-" + row._2)
      .map(row => (row._1, 1))
      .reduceByKey(_ + _)
    if (log.isDebugEnabled) {
      rank.take(10).foreach(println)
    }

    // 更新排行
    rank.coalesce(parallelism)
      .foreachPartition { iterator =>
        val sqls = iterator.map { row =>
          val arr = row._1.split("-")
          s"INSERT INTO t_fan_apply_header_filter_record (star_id, user_id, fan_month_num) VALUES (${arr(0)}, ${arr(1)}, ${row._2}) ON DUPLICATE KEY UPDATE fan_month_num = ${row._2}"
        }
        if (sqls.nonEmpty) JdbcUtil.executeBatch(starDb, sqls.toArray)
      }

    // 粉丝申请圈主判断
    val sqls = Array("UPDATE t_fan_apply_header_filter_record SET create_time = NOW() WHERE create_time = 0",
      "UPDATE t_fan_apply_header_filter_record SET update_time = create_time WHERE update_time = 0",
      "UPDATE t_fan_apply_header_filter_record SET create_by = NULL WHERE create_by = 0",
      "UPDATE t_fan_apply_header_filter_record SET focus_time = NULL WHERE focus_time = 0",
      "UPDATE t_fan_apply_header_filter_record SET is_satisfy_header = 1 WHERE focus_time < CURDATE() - INTERVAL 30 DAY AND fan_dynamic_num >= 30 AND (fan_dynamic_top_num + fan_dynamic_cream_num) >= 2 AND fan_month_num >= 1")
    JdbcUtil.executeBatch(starDb, sqls)
  }
}