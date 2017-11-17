package com.jiuzhi.etl.fad

import org.zc.sched.plugins.spark.TaskExecutor
import org.zc.sched.model.Task

class SDKVersion(task: Task) extends TaskExecutor(task) with Serializable {

  val paths = "/flume/advs/2017-08-15/topic_ad_visit,/flume/advs/2017-08-16/topic_ad_visit,/flume/advs/2017-08-17/topic_ad_visit,/flume/advs/2017-08-18/topic_ad_visit,/flume/advs/2017-08-19/topic_ad_visit,/flume/advs/2017-08-20/topic_ad_visit,/flume/advs/2017-08-21/topic_ad_visit,/flume/advs/2017-08-22/topic_ad_visit,/flume/advs/2017-08-23/topic_ad_visit,/flume/advs/2017-08-24/topic_ad_visit,/flume/advs/2017-08-25/topic_ad_visit,/flume/advs/2017-08-26/topic_ad_visit,/flume/advs/2017-08-27/topic_ad_visit,/flume/advs/2017-08-28/topic_ad_visit,/flume/advs/2017-08-29/topic_ad_visit,/flume/advs/2017-08-30/topic_ad_visit,/flume/advs/2017-08-31/topic_ad_visit,/flume/advs/2017-09-01/topic_ad_visit,/flume/advs/2017-09-02/topic_ad_visit,/flume/advs/2017-09-03/topic_ad_visit,/flume/advs/2017-09-04/topic_ad_visit,/flume/advs/2017-09-05/topic_ad_visit,/flume/advs/2017-09-06/topic_ad_visit,/flume/advs/2017-09-07/topic_ad_visit,/flume/advs/2017-09-08/topic_ad_visit,/flume/advs/2017-09-09/topic_ad_visit,/flume/advs/2017-09-10/topic_ad_visit,/flume/advs/2017-09-11/topic_ad_visit,/flume/advs/2017-09-12/topic_ad_visit,/flume/advs/2017-09-13/topic_ad_visit,/flume/advs/2017-09-14/topic_ad_visit,/flume/advs/2017-09-15/topic_ad_visit,/flume/advs/2017-09-16/topic_ad_visit,/flume/advs/2017-09-17/topic_ad_visit,/flume/advs/2017-09-18/topic_ad_visit,/flume/advs/2017-09-19/topic_ad_visit,/flume/advs/2017-09-20/topic_ad_visit,/flume/advs/2017-09-21/topic_ad_visit,/flume/advs/2017-09-22/topic_ad_visit,/flume/advs/2017-09-23/topic_ad_visit,/flume/advs/2017-09-24/topic_ad_visit,/flume/advs/2017-09-25/topic_ad_visit,/flume/advs/2017-09-26/topic_ad_visit,/flume/advs/2017-09-27/topic_ad_visit,/flume/advs/2017-09-28/topic_ad_visit,/flume/advs/2017-09-29/topic_ad_visit,/flume/advs/2017-09-30/topic_ad_visit,/flume/advs/2017-10-01/topic_ad_visit,/flume/advs/2017-10-02/topic_ad_visit,/flume/advs/2017-10-03/topic_ad_visit,/flume/advs/2017-10-04/topic_ad_visit,/flume/advs/2017-10-05/topic_ad_visit,/flume/advs/2017-10-06/topic_ad_visit,/flume/advs/2017-10-07/topic_ad_visit,/flume/advs/2017-10-08/topic_ad_visit,/flume/advs/2017-10-09/topic_ad_visit,/flume/advs/2017-10-10/topic_ad_visit"

  val paths2 = "/flume/advs2/2017-09-27/topic_ad_visit,/flume/advs2/2017-09-28/topic_ad_visit,/flume/advs2/2017-09-29/topic_ad_visit,/flume/advs2/2017-09-30/topic_ad_visit,/flume/advs2/2017-10-01/topic_ad_visit,/flume/advs2/2017-10-02/topic_ad_visit,/flume/advs2/2017-10-03/topic_ad_visit,/flume/advs2/2017-10-04/topic_ad_visit,/flume/advs2/2017-10-05/topic_ad_visit,/flume/advs2/2017-10-06/topic_ad_visit,/flume/advs2/2017-10-07/topic_ad_visit,/flume/advs2/2017-10-08/topic_ad_visit,/flume/advs2/2017-10-09/topic_ad_visit,/flume/advs2/2017-10-10/topic_ad_visit"

  val json = spark.read.json(paths2.split(","): _*).select("udid", "apppkg", "sdkver", "createtime").na.drop(Array("udid", "apppkg", "sdkver")).where("udid > '' AND apppkg > '' AND sdkver > ''")

  val result = json.rdd.groupBy { row => row.getString(0) + row.getString(1) }.map { row =>
    val item = row._2.toSeq.sortBy(_.getString(3))
    val head = item.head
    val tail = item.last
    (head.getString(0), head.getString(1), head.getString(2), tail.getString(2))
  }

  result.coalesce(1).saveAsTextFile("/tmp/sdkver")

  def execute {
  }

}