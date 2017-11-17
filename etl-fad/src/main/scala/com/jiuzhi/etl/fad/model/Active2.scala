package com.jiuzhi.etl.fad.model

import java.sql.Timestamp

import org.apache.spark.sql.Row

case class Active2(udid: String, app_key: String, active_date: String, log_type: Int, version: String,
  city_id: Long, country: String, create_time: Timestamp, var visit_times: Int = 1)

object Active2 {

  def apply(row: Row): Active2 = {
    Active2(row.getString(0), row.getString(1), row.getString(2), row.getInt(3), row.getString(4),
      row.getLong(5), row.getString(6), row.getTimestamp(7))
  }

  def update(acc: Active2, curr: Active2) = {
    acc.visit_times = acc.visit_times + curr.visit_times

    acc
  }

}