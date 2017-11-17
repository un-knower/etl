package com.jiuzhi.etl.sdk.model

import java.sql.Timestamp

import org.apache.spark.sql.Row

case class Active(uuid: String, app_key: String, active_date: String, log_type: Int, var version: String,
  city: String, region: String, country: String, create_time: Timestamp, var visit_times: Int = 1)

object Active {

  def apply(row: Row): Active = {
    Active(row.getString(0), row.getString(1), row.getString(2), row.getInt(3), row.getString(4),
      row.getString(5), row.getString(6), row.getString(7), row.getTimestamp(8))
  }

  def update(acc: Active, curr: Active) = {
    acc.visit_times = acc.visit_times + curr.visit_times

    acc
  }

}