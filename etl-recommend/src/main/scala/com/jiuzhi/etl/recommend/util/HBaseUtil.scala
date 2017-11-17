package com.jiuzhi.etl.recommend.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

object HBaseUtil {

  /**
   * 读HBase
   */
  def readHBase(conf: Configuration, sc: SparkContext) = {
    val hbaseConf = HBaseConfiguration.create(conf)
    hbaseConf.addResource(sc.hadoopConfiguration)
    sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
  }

  /**
   * 写HBase
   */
  def writeHBase(conf: Configuration, rdd: RDD[Put]) {
    val hbaseConf = HBaseConfiguration.create(conf)
    rdd.map((new ImmutableBytesWritable, _)).saveAsNewAPIHadoopDataset(hbaseConf)
  }

}