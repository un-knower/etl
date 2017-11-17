package com.jiuzhi.etl.sdk.model

import java.sql.Timestamp
import org.apache.spark.sql.Row

import org.zc.sched.util.DateUtil

case class Client(uuid: String, app_key: String, customer_id: String, var version: String, pkg_path: Int,
  create_time: Timestamp, var update_time: Timestamp, init_version: String, create_date: Int, var is_upgrade: Int = 0,
  var is_silent: Int = 0)

object Client {

  def apply(row: Row): Client = {
    Client(row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getInt(4),
      row.getTimestamp(5), row.getTimestamp(6), row.getString(7), row.getInt(8))
  }

  def update(acc: Client, curr: Client) = {
    acc.version = curr.version
    acc.update_time = curr.update_time

    acc
  }

  def finalize(client: Client): Client = {
    // 是否升级判断
    if (client.version != client.init_version) client.is_upgrade = 1

    // 沉默用户判断
    // 创建日期与当前日期间隔天数
    val dateDiff1 = DateUtil.intervalDays(true, client.create_time)
    // 创建日期与更新日期间隔天数
    val dateDiff2 = DateUtil.intervalDays(true, client.create_time, client.update_time)
    if (dateDiff1 >= 3 && dateDiff2 <= 1) client.is_silent = 1

    client
  }

}