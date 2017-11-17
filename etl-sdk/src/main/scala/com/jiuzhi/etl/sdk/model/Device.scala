package com.jiuzhi.etl.sdk.model

import java.sql.Timestamp

import org.apache.spark.sql.Row

case class Device(uuid: String, device_id: String, app_key: String, customer_id: String, var network: String,
  platform: String, var have_vpn: Int, var imsi: String, var wifi_mac: String, imei: String,
  android_id: String, baseband: String, language: String, resolution: String, model_name: String,
  cpu: String, device_name: String, os_version: String, cameras: String, sdcard_size: Long,
  rom_size: Long, var phone_no: String, city: String, region: String, country: String,
  uuid_type: Int, var isp_code: Int, create_time: Timestamp, var update_time: Timestamp)

object Device {

  def apply(row: Row): Device = {
    Device(row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4),
      row.getString(5), row.getInt(6), row.getString(7), row.getString(8), row.getString(9),
      row.getString(10), row.getString(11), row.getString(12), row.getString(13), row.getString(14),
      row.getString(15), row.getString(16), row.getString(17), row.getString(18), row.getLong(19),
      row.getLong(20), row.getString(21), row.getString(22), row.getString(23), row.getString(24),
      row.getInt(25), row.getInt(26), row.getTimestamp(27), row.getTimestamp(28))
  }

  def update(acc: Device, curr: Device): Device = {
    if (acc.network < curr.network) acc.network = curr.network
    if (acc.have_vpn != 1) acc.have_vpn = curr.have_vpn
    if (acc.imsi == null || "null".equalsIgnoreCase(acc.imsi) || "".equals(acc.imsi)) acc.imsi = curr.imsi
    if (acc.wifi_mac == null || "null".equalsIgnoreCase(acc.wifi_mac) || "".equals(acc.wifi_mac)) acc.wifi_mac = curr.wifi_mac
    if (acc.phone_no == null || "null".equalsIgnoreCase(acc.phone_no) || "".equals(acc.phone_no)) acc.phone_no = curr.phone_no
    acc.update_time = curr.update_time

    acc
  }

  def finalize(device: Device): Device = {
    if (device.network < "2") device.network = device.network.substring(1)
    try {
      device.isp_code = device.imsi.substring(0, 5).toInt
    } catch {
      case _: Throwable =>
    }

    device
  }

}