package com.jiuzhi.etl.recommend

import java.util.Properties

import collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.DateUtils

import com.jiuzhi.etl.recommend.util.HBaseUtil

/**
 * 推荐程序
 * 按栏目权重推荐
 */
class UserRecommend2(task: Task) extends TaskExecutor(task) with Serializable {

  // 活跃用户数据库
  val userDB = getDbConn(task.taskExt.get("user_db_id").get.toInt).get
  // 资讯数据库
  val infoDB = getDbConn(task.taskExt.get("info_db_id").get.toInt).get

  // 推荐结果TTL(单位为毫秒)
  val recommendTTL = task.taskExt.getOrElse("recommend_ttl", 1000 * 3600 * 24 * 3).toString().toLong

  // 用户标签表
  val userTable = task.taskExt.getOrElse("user_table", "user_profile")
  // 推荐结果表
  val recommendTable = task.taskExt.getOrElse("recommend_table", "user_recommend")

  // 最近几天活跃用户
  val dayActive = task.taskExt.getOrElse("day_active", 7).toString().toInt
  // 最近几小时资讯
  val hourLatest = task.taskExt.getOrElse("hour_latest", 72).toString().toInt

  // 用户数据按多少小时分一个区
  val partitionHour = task.taskExt.getOrElse("partition_hour", 8).toString().toInt

  // 每个用户一次推荐多少条资讯
  val recommendNum = task.taskExt.getOrElse("recommend_num", 100).toString().toInt

  // 推荐用户数限制
  val userLimit = task.taskExt.getOrElse("user_limit", 10000000).toString().toInt
  // 推荐资讯数限制
  val infoLimit = task.taskExt.getOrElse("info_limit", 100000).toString().toInt

  // HBase Zookeeper Quorum
  val hBaseZKURL = task.taskExt.get("hbase_zk_url").get
  val hBaseTimeout = task.taskExt.getOrElse("hbase_timeout", 120000).toString().toInt

  def execute {
    // 从MySQL读取最近几天活跃用户(按小时分区并行读取)
    log.info("Get latest active user from mysql table: active_device")
    val predicates = DateUtils.rangeHour(partitionHour, DateUtil.nextDate(-dayActive)).map { row =>
      val startTime = DateUtil.formatDatetime(row._1)
      val endTime = DateUtil.formatDatetime(row._2)
      s"active_time >= '${startTime}' AND active_time < '${endTime}'"
    }
    val activeUser = spark.read.jdbc(userDB.jdbcUrl, "active_device", predicates, userDB.connProps)
      .select("device_id")
      .limit(userLimit).rdd
      .map(row => (row.getString(0), 1))
      .collectAsMap()
    if (log.isDebugEnabled()) log.debug("Active user count: " + activeUser.size)

    // 广播活跃用户
    val userBC = spark.sparkContext.broadcast(activeUser)

    // 从HBase读取用户标签
    // 关联活跃用户取交集
    log.info(s"Get user profile from hbase table: ${userTable}")
    val hconfig = new Configuration
    hconfig.set(HConstants.ZOOKEEPER_QUORUM, hBaseZKURL)
    hconfig.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, hBaseTimeout)
    hconfig.set(TableInputFormat.INPUT_TABLE, userTable)
    hconfig.set(TableInputFormat.SCAN_COLUMNS, "tag:wtags")
    val userProfile = HBaseUtil.readHBase(hconfig, spark.sparkContext).mapPartitions { iterator =>
      val userMap = userBC.value
      for {
        (row, result) <- iterator
        val key = Bytes.toString(row.get)
        if (userMap.contains(key))
      } yield {
        val value = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("tag"), Bytes.toBytes("wtags"))))
        (key, value.split(",").map { tag =>
          val arr = tag.split(":")
          (arr(0).toLong, arr(1).toDouble)
        })
      }
    }.coalesce(parallelism)
    if (log.isDebugEnabled()) log.debug("Recommend user count: " + userProfile.count)

    // 从MySQL读取最新资讯
    log.info("Get latest information from mysql table: t_information")
    val publishTime = DateUtil.formatDatetime(DateUtil.nextHour(-hourLatest))
    val newInfo = spark.read.jdbc(infoDB.jdbcUrl, "t_information", Array(s"publish_time > '${publishTime}' AND publish_time < NOW() LIMIT ${infoLimit}"), infoDB.connProps)
      .select("id", "category_id", "publish_time").rdd
      .map(row => (row.getLong(0), row.getLong(1), row.getTimestamp(2).getTime))
      .collect()
    if (log.isDebugEnabled()) log.debug("Latest information count: " + newInfo.size)

    // 广播最新资讯
    val infoBC = spark.sparkContext.broadcast(newInfo)

    // 给每个用户推荐资讯(取用户标签包含的分类)
    log.info("Recommend information for each user")
    val recommendations = userProfile.mapPartitions { iterator =>
      val infoList = infoBC.value
      iterator.flatMap { user =>
        val userProfile = user._2.toMap
        for {
          (info_id, category_id, publish_time) <- infoList
          if (userProfile.contains(category_id))
        } yield (user._1, (info_id, (category_id, userProfile.get(category_id).get, publish_time)))
      }
    }.aggregateByKey(Array[(Long, (Long, Double, Long))]())(_ :+ _, _ ++ _)
    if (log.isDebugEnabled()) log.debug("Recommended user count: " + recommendations.count)

    // 从HBase读取已推资讯
    log.info(s"Get pushed information from hbase table: ${recommendTable}")
    val hconfig1 = new Configuration
    hconfig1.set(HConstants.ZOOKEEPER_QUORUM, hBaseZKURL)
    hconfig1.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, hBaseTimeout)
    hconfig1.set(TableInputFormat.INPUT_TABLE, recommendTable)
    hconfig1.set(TableInputFormat.SCAN_COLUMN_FAMILY, "recmd")
    hconfig1.setInt(TableInputFormat.SCAN_MAXVERSIONS, Integer.MAX_VALUE)
    val filterList = HBaseUtil.readHBase(hconfig1, spark.sparkContext).map { row =>
      val list = ArrayBuffer[Long]()
      // 待推荐资讯
      val tip = row._2.getColumnCells(Bytes.toBytes("recmd"), Bytes.toBytes("tip")).iterator()
      while (tip.hasNext()) {
        val id = Bytes.toLong(CellUtil.cloneValue(tip.next()))
        list += id
      }

      // 已推荐资讯
      val his = row._2.getColumnCells(Bytes.toBytes("recmd"), Bytes.toBytes("his")).iterator()
      while (his.hasNext()) {
        val ids = Bytes.toString(CellUtil.cloneValue(his.next())).split(",").map(_.toLong)
        list ++= ids
      }

      (Bytes.toString(row._1.get), list.toArray.distinct)
    }

    // 过滤已推荐资讯
    // 按分类分组后按发布时间排序取topN
    log.info("Filter pushed information from recommendation list")
    val result = recommendations.leftOuterJoin(filterList).flatMap { row =>
      val recommendList = row._2._1.toMap
      val pushList = row._2._2.getOrElse(Array())
      val result = recommendList.--(pushList).map { info =>
        ((info._2._1, info._2._2), info._1, info._2._3)
      }.groupBy(_._1).flatMap { category =>
        val num = (category._1._2 * recommendNum).round.intValue()
        category._2.map { info =>
          (info._2, info._3)
        }.toList.sortWith((bigger, smaller) => bigger._2 > smaller._2).take(num)
      }

      // 发布时间添加毫秒作为时间戳,避免版本冲突
      var num = 999
      result.toList.sortWith((bigger, smaller) => bigger._2 > smaller._2).map { item =>
        val put = new Put(Bytes.toBytes(row._1))
        put.addColumn(Bytes.toBytes("recmd"), Bytes.toBytes("tip"), item._2 + num, Bytes.toBytes(item._1))
        put.setTTL(recommendTTL)
        num -= 1
        put
      }
    }.coalesce(parallelism)
    if (log.isDebugEnabled()) log.debug("Final recommendation count: " + result.count)

    // 把推荐结果写入HBase
    log.info(s"Write final recommendation result to hbase table: ${recommendTable}")
    val hconfig2 = new Configuration
    hconfig2.set(HConstants.ZOOKEEPER_QUORUM, hBaseZKURL)
    hconfig2.set(TableOutputFormat.OUTPUT_TABLE, recommendTable)
    hconfig2.addResource(spark.sparkContext.hadoopConfiguration)
    val job = Job.getInstance(hconfig2)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    HBaseUtil.writeHBase(job.getConfiguration, result)
  }
}