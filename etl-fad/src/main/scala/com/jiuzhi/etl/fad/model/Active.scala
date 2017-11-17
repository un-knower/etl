package com.jiuzhi.etl.fad.model

import java.sql.Timestamp

import org.apache.spark.sql.Row

case class Active(udid: String, app_key: String, active_date: String, version: String, city_id: Long,
  country: String, create_time: Timestamp, var visit_times: Int = 1)

object Active {

  def apply(row: Row): Active = {
    Active(row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getLong(4),
      row.getString(5), row.getTimestamp(6))
  }

  def update(acc: Active, curr: Active) = {
    acc.visit_times = acc.visit_times + curr.visit_times

    acc
  }

}