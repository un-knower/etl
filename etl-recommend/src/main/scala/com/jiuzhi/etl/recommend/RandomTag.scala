package com.jiuzhi.etl.recommend

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task
import org.zc.sched.util.JdbcUtil

import com.jiuzhi.etl.recommend.util.HBaseUtil

/**
 * 随机生成用户标签
 * 标签用户统计
 */
class RandomTag(task: Task) extends TaskExecutor(task) with Serializable {

  // 用户标签表
  val userTable = task.taskExt.getOrElse("user_table", "user_profile")

  // HBase Zookeeper Quorum
  val hBaseZKURL = task.taskExt.get("hbase_zk_url").get
  val hBaseTimeout = task.taskExt.getOrElse("hbase_timeout", 120000).toString.toInt

  // 取前几个标签
  val topN = task.taskExt.getOrElse("topn_tag", 3).toString.toInt

  // 统计数据库
  val statDb = getDbConn(task.taskExt.get("stat_db_id").get.toInt).get

  def execute {
    // 从HBase读取用户标签
    // 从用户标签中随机取一个
    log.info(s"Get user profile from hbase table: ${userTable}")
    val hconfig = new Configuration
    hconfig.set(HConstants.ZOOKEEPER_QUORUM, hBaseZKURL)
    hconfig.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, hBaseTimeout)
    hconfig.set(TableInputFormat.INPUT_TABLE, userTable)
    hconfig.set(TableInputFormat.SCAN_COLUMNS, "tag:wtags")
    val randomTag = HBaseUtil.readHBase(hconfig, spark.sparkContext).mapPartitions { iterator =>
      iterator.map { row =>
        val key = Bytes.toString(row._1.get)
        val value = Bytes.toString(CellUtil.cloneValue(row._2.getColumnLatestCell(Bytes.toBytes("tag"), Bytes.toBytes("wtags"))))
        val arr = value.split(",").map { tag =>
          val arr = tag.split(":")
          (arr(0).toLong, arr(1).toDouble)
        }
        val n = new scala.util.Random().nextInt(Math.min(topN, arr.length))

        (key, arr(n)._1)
      }
    }

    // 写入HBase
    val userTag = randomTag.map { row =>
      val put = new Put(Bytes.toBytes(row._1))
      put.addColumn(Bytes.toBytes("tag"), Bytes.toBytes("rtag"), Bytes.toBytes(row._2))
      put
    }.coalesce(parallelism)
    val hconfig2 = new Configuration
    hconfig2.set(HConstants.ZOOKEEPER_QUORUM, hBaseZKURL)
    hconfig2.set(TableOutputFormat.OUTPUT_TABLE, userTable)
    hconfig2.addResource(spark.sparkContext.hadoopConfiguration)
    val job = Job.getInstance(hconfig2)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    HBaseUtil.writeHBase(job.getConfiguration, userTag)

    import spark.implicits._

    // 统计标签人数
    val result = randomTag.map(row => (row._2, 1)).reduceByKey(_ + _)
      .toDF("tag_id", "user_count")
      .withColumn("stat_date", lit(s"${task.statDate}"))
      .coalesce(parallelism)
    if (log.isDebugEnabled) {
      result.printSchema
      result.show(50, false)
    }
    JdbcUtil.executeUpdate(statDb, s"DELETE FROM stat_user_tag WHERE stat_date = '${task.statDate}'")
    result.write.mode(SaveMode.Append).jdbc(statDb.jdbcUrl, "stat_user_tag", statDb.connProps)
  }
}