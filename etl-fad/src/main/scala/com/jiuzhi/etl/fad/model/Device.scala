package com.jiuzhi.etl.fad.model

import java.sql.Timestamp

import org.apache.spark.sql.Row

case class Device(udid: String, deviceid: String, var imsi: String, imei: String, vender: String,
  model: String, os_version: String, platform: String, android_id: String, operator: String,
  var network: String, src: String, mac: String, app_key: String, clnt: String,
  is_root: Int, has_gplay: Int, gaid: String, rom: Long, lang: String,
  ua: String, city_id: Long, country: String, create_time: Timestamp, var update_time: Timestamp)

object Device {

  def apply(row: Row): Device = {
    Device(row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4),
      row.getString(5), row.getString(6), row.getString(7), row.getString(8), row.getString(9),
      row.getString(10), row.getString(11), row.getString(12), row.getString(13), row.getString(14),
      row.getInt(15), row.getInt(16), row.getString(17), row.getLong(18), row.getString(19),
      row.getString(20), row.getLong(21), row.getString(22), row.getTimestamp(23), row.getTimestamp(24))
  }

  def update(acc: Device, curr: Device): Device = {
    if ("null".equalsIgnoreCase(acc.imsi) || "".equals(acc.imsi)) acc.imsi = curr.imsi
    if (acc.network < curr.network) acc.network = curr.network
    acc.update_time = curr.update_time

    acc
  }

  def finalize(device: Device): Device = {
    if (device.network < "2") device.network = device.network.substring(1)

    device
  }

}