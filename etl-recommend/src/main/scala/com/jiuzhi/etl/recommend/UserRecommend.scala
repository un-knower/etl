package com.jiuzhi.etl.recommend

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import redis.clients.jedis.Jedis

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.DateUtil
import org.zc.sched.util.DateUtils

import com.jiuzhi.etl.recommend.util.HBaseUtil
import com.jiuzhi.etl.recommend.util.SimiUtil

/**
 * 离线推荐程序
 * 按余弦相似度推荐
 * 推荐结果用redis list存储
 * 推荐结果按发布时间降序排列，然后分页，每页资讯ID之间用逗号隔开
 */
class UserRecommend(task: Task) extends TaskExecutor(task) with Serializable {

  // 活跃用户数据库
  val userDB = getDbConn(task.taskExt.get("user_db_id").get.toInt).get
  // 资讯数据库
  val infoDB = getDbConn(task.taskExt.get("info_db_id").get.toInt).get

  // 活跃用户表
  val activeTable = task.taskExt.getOrElse("active_table", "active_device")
  // 资讯表
  val infoTable = task.taskExt.getOrElse("info_table", "t_information")
  // 用户标签表
  val userTable = task.taskExt.getOrElse("user_table", "user_profile")

  // 推荐结果TTL
  val recommendTTL = task.taskExt.getOrElse("recommend_ttl", 3600 * 24 * 3).toString.toInt
  // 分页大小
  val pageSize = task.taskExt.getOrElse("page_size", 10).toString.toInt

  // 最近几天活跃用户
  val dayActive = task.taskExt.getOrElse("day_active", 7).toString().toInt
  // 最近几小时资讯
  val hourLatest = task.taskExt.getOrElse("hour_latest", 72).toString().toInt

  // 用户数据按多少小时分一个区
  val partitionHour = task.taskExt.getOrElse("partition_hour", 8).toString.toInt

  // 每个用户一次推荐多少条资讯
  val recommendNum = task.taskExt.getOrElse("recommend_num", 100).toString.toInt

  // 推荐用户数限制
  val userLimit = task.taskExt.getOrElse("user_limit", 10000000).toString.toInt
  // 推荐资讯数限制
  val infoLimit = task.taskExt.getOrElse("info_limit", 100000).toString.toInt

  // HBase Zookeeper Quorum
  val hBaseZKURL = task.taskExt.get("hbase_zk_url").get
  val hBaseTimeout = task.taskExt.getOrElse("hbase_timeout", 120000).toString.toInt

  // redis
  val redisHost = task.taskExt.getOrElse("redis_host", "localhost")
  val redisPort = task.taskExt.getOrElse("redis_port", 6379).toString.toInt
  val redisPasswd = task.taskExt.getOrElse("redis_passwd", "foobared")

  def execute {
    // 从MySQL读取最近几天活跃用户(按小时分区并行读取)
    log.info(s"Get latest active user from mysql table: ${activeTable}")
    val predicates = DateUtils.rangeHour(partitionHour, DateUtil.nextDate(-dayActive), DateUtil.nextHour(1)).map { row =>
      val startTime = DateUtil.formatDatetime(row._1)
      val endTime = DateUtil.formatDatetime(row._2)
      s"active_time >= '${startTime}' AND active_time < '${endTime}' ORDER BY active_time DESC LIMIT ${userLimit}"
    }
    val activeUser = spark.read.jdbc(userDB.jdbcUrl, activeTable, predicates, userDB.connProps)
      .orderBy("active_time")
      .limit(userLimit)
      .select("device_id")
      .rdd
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
    hconfig.set(TableInputFormat.SCAN_COLUMNS, "tag:tags")
    val userProfile = HBaseUtil.readHBase(hconfig, spark.sparkContext).mapPartitions { iterator =>
      val userMap = userBC.value
      for {
        (row, result) <- iterator
        val key = Bytes.toString(row.get)
        if (userMap.contains(key))
      } yield {
        val value = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("tag"), Bytes.toBytes("tags"))))
        (key, value.split(",").map { tag =>
          val arr = tag.split(":")
          (arr(0).toLong, arr(1).toDouble)
        })
      }
    }.coalesce(parallelism)
    if (log.isDebugEnabled()) log.debug("Recommend user count: " + userProfile.count)

    // 从MySQL读取最新资讯
    log.info(s"Get latest information from mysql table: ${infoTable}")
    val publishTime = DateUtil.formatDatetime(DateUtil.nextHour(-hourLatest))
    val newInfo = spark.read.jdbc(infoDB.jdbcUrl, infoTable, Array(s"publish_time > '${publishTime}' AND publish_time < NOW() AND idwords > '' ORDER BY publish_time DESC LIMIT ${infoLimit}"), infoDB.connProps)
      .select("id", "idwords", "publish_time")
      .rdd
      .map { row =>
        val tags = row.getString(1).split(",").map { tag =>
          val arr = tag.split(":")
          (arr(0).toLong, arr(1).toDouble)
        }
        (row.getLong(0), tags, row.getTimestamp(2).getTime)
      }
      .collect()
    if (log.isDebugEnabled()) log.debug("Latest information count: " + newInfo.size)

    // 广播最新资讯
    val infoBC = spark.sparkContext.broadcast(newInfo)

    // 给每个用户推荐资讯
    log.info("Recommend information for each user")
    val recommendations = userProfile.mapPartitions { iterator =>
      val infoList = infoBC.value
      iterator.flatMap { user =>
        for {
          (info_id, tags, publish_time) <- infoList
          val similarity = SimiUtil.cos_sim(user._2.toMap, tags.toMap)
          if (similarity > 0)
        } yield (user._1, (info_id, (similarity, publish_time)))
      }
    }.aggregateByKey(Array[(Long, (Double, Long))]())(_ :+ _, _ ++ _)
    if (log.isDebugEnabled()) log.debug("Recommended user count: " + recommendations.count)

    // 过滤已推荐资讯
    // 排序取topN
    // 写入redis
    recommendations.foreachPartition { iterator =>
      // 连接redis
      val jedis = new Jedis(redisHost, redisPort)
      jedis.auth(redisPasswd)

      iterator.foreach { user =>
        val deviceId = user._1
        // 读取已推荐资讯
        val hisKey = "his-" + deviceId
        // java.util.List => scala.collection.mutable.Buffer
        import scala.collection.JavaConverters._
        val hisList = jedis.lrange(hisKey, 0, -1).asScala.flatMap(_.split(",").map(_.toLong))

        // 过滤后按相似度和发布时间降序排列取topN
        // 按发布时间和相似度降序排列
        val result = user._2.toMap.--(hisList).toList.sortWith { (bigger, smaller) =>
          val flag1 = bigger._2._1 == smaller._2._1
          val flag2 = bigger._2._2 == smaller._2._2
          if (!flag1) {
            bigger._2._1 > smaller._2._1
          } else if (!flag2) {
            bigger._2._2 > smaller._2._2
          } else {
            bigger._1 > smaller._1
          }
        }.take(recommendNum).sortWith { (bigger, smaller) =>
          val flag1 = bigger._2._1 == smaller._2._1
          val flag2 = bigger._2._2 == smaller._2._2
          if (!flag2) {
            bigger._2._2 > smaller._2._2
          } else if (!flag1) {
            bigger._2._1 > smaller._2._1
          } else {
            bigger._1 > smaller._1
          }
        }.map(_._1)

        val totalCount = result.length
        if (totalCount > 0) {
          val tipKey = "tip-" + deviceId
          // 分页
          val totalPage = totalCount / pageSize + math.min(totalCount % pageSize, 1)
          var tmp = ""
          for (i <- 1 to totalPage) {
            val page = result.slice((i - 1) * pageSize, i * pageSize).mkString(",")
            tmp = tmp.concat(page).concat(" ")
          }
          // 推荐结果写入redis
          jedis.lpush(tipKey, tmp.split(" "): _*)
          // 清除上次推荐的资讯
          jedis.ltrim(tipKey, 0, totalPage - 1)
          // 设置TTL
          jedis.expire(tipKey, recommendTTL)
        }
      }
      // 关闭连接
      jedis.close
    }
  }
}