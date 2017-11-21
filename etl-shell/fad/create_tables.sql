-- 每次更新前面加两个# 首次有效值更新前面加一个#

-- 设备信息
CREATE TABLE IF NOT EXISTS dim_device (
  udid VARCHAR(50) COMMENT '校验设备ID',
  deviceid VARCHAR(50) COMMENT '客户端设备号',
  imsi VARCHAR(50) COMMENT '#imsi',
  imei VARCHAR(50) COMMENT 'imei',
  vender VARCHAR(100) COMMENT '设备厂商',
  model VARCHAR(100) COMMENT '机器型号',
  os_version VARCHAR(20) COMMENT '操作系统版本',
  platform VARCHAR(20) COMMENT 'ios, android',
  android_id VARCHAR(50),
  operator VARCHAR(50) COMMENT '运营商 1:移动, 2:联通, 3:电信, 4:其他',
  network VARCHAR(20) COMMENT '##最高网络类型 Unknown<WIFI<GPRS<2G<3G<4G',
  src VARCHAR(20) COMMENT '分辨率',
  mac VARCHAR(50) COMMENT 'mac地址',
  app_key VARCHAR(50) COMMENT '首个产品key',
  clnt VARCHAR(20) COMMENT '初始渠道号',
  is_root TINYINT(4) COMMENT '客户端权限 1:root, 2:非root',
  has_gplay TINYINT(4) COMMENT '1:已安装google play, 2:未安装google play',
  gaid VARCHAR(50) COMMENT 'google advertising id',
  rom BIGINT(20) COMMENT '内存大小(单位M)',
  lang VARCHAR(100) COMMENT '系统语言',
  ua VARCHAR(200) COMMENT '浏览器引擎',
  city_id BIGINT(20) COMMENT '城市编号',
  country VARCHAR(50) COMMENT '国家',
  create_time DATETIME COMMENT '创建时间',
  update_time DATETIME COMMENT '##更新时间',
  PRIMARY KEY (udid)
) COMMENT='设备信息';

-- 客户端信息
CREATE TABLE IF NOT EXISTS fact_client (
  udid VARCHAR(50) COMMENT '校验设备ID',
  app_key VARCHAR(50) COMMENT '产品key',
  clnt VARCHAR(20) COMMENT '初始渠道号',
  version VARCHAR(20) COMMENT '##应用当前版本',
  init_version VARCHAR(20) COMMENT '应用初始版本',
  app_path TINYINT(4) COMMENT '客户端目录 1:system, 2:data, 3:sd卡, 4:vendor, 5:system priv',
  create_time DATETIME COMMENT '创建时间',
  update_time DATETIME COMMENT '##更新时间',
  create_date INT(11) COMMENT '激活日期',
  PRIMARY KEY (udid, app_key)
) COMMENT='客户端信息';

-- 活跃用户
CREATE TABLE IF NOT EXISTS fact_active (
  udid VARCHAR(50) COMMENT '设备ID',
  app_key VARCHAR(50) COMMENT '产品key',
  active_date INT(11) COMMENT '活跃日期',
  version VARCHAR(20) COMMENT '应用当前版本',
  city_id BIGINT(20) COMMENT '城市编号',
  country VARCHAR(50) COMMENT '国家',
  visit_times INT(11) COMMENT '访问次数',
  clnt VARCHAR(20) COMMENT '渠道号',
  init_version VARCHAR(20) COMMENT '应用初始版本',
  create_date INT(11) COMMENT '创建日期',
  date_diff INT(11) COMMENT '第几天活跃',
  PRIMARY KEY (udid, app_key, active_date)
) COMMENT='活跃用户';

-- 用户使用App版本记录
-- hive
CREATE TABLE IF NOT EXISTS mid_version (
  udid VARCHAR(50) COMMENT '设备ID',
  app_key VARCHAR(50) COMMENT '产品key',
  version VARCHAR(20) COMMENT '应用当前版本',
  create_time TIMESTAMP COMMENT '首次使用时间'
) COMMENT '用户使用App版本记录' PARTITIONED BY (stat_date STRING) STORED AS PARQUET;

-- 升级下发日志
-- hive
CREATE TABLE IF NOT EXISTS log_upgrade (
  udid VARCHAR(50) COMMENT '设备ID',
  app_key VARCHAR(50) COMMENT '产品key',
  version VARCHAR(20) COMMENT '应用当前版本',
  up_version VARCHAR(20) COMMENT '应用升级版本',
  create_date DATE COMMENT '升级下发日期'
) COMMENT '升级日志' PARTITIONED BY (stat_date STRING) STORED AS PARQUET;

-- 升级信息
-- hive
CREATE TABLE IF NOT EXISTS mid_upgrade (
  udid VARCHAR(50) COMMENT '设备ID',
  app_key VARCHAR(50) COMMENT '产品key',
  version VARCHAR(20) COMMENT '应用当前版本',
  up_version VARCHAR(20) COMMENT '应用升级版本',
  create_date DATE COMMENT '升级下发日期',
  upgrade_date DATE COMMENT '升级成功日期'
) COMMENT '升级信息' PARTITIONED BY (stat_date STRING) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS fact_upgrade (
  udid VARCHAR(50) COMMENT '设备ID',
  app_key VARCHAR(50) COMMENT '产品key',
  version VARCHAR(20) COMMENT '应用当前版本',
  up_version VARCHAR(20) COMMENT '应用升级版本',
  create_date INT(11) COMMENT '升级下发日期',
  upgrade_date INT(11) COMMENT '升级成功日期',
  clnt VARCHAR(20) COMMENT '渠道号',
  PRIMARY KEY (udid, app_key, version, up_version)
) COMMENT='升级信息';

-- 运营分析
CREATE TABLE IF NOT EXISTS fact_runlevel (
  app_key VARCHAR(50) COMMENT '产品key',
  clnt VARCHAR(20) COMMENT '渠道号',
  runlevel INT(11) COMMENT '过滤条件',
  create_date INT(11) COMMENT '创建日期',
  user_count INT(11) COMMENT '用户数',
  PRIMARY KEY(app_key, clnt, runlevel, create_date)
) COMMENT='运营分析';

-- 黑名单分析
CREATE TABLE IF NOT EXISTS fact_blacklist (
  stat_date INT(11) COMMENT '统计日期',
  app_key VARCHAR(50) COMMENT '产品key',
  clnt VARCHAR(20) COMMENT '渠道号',
  black_count INT(11) COMMENT '黑名单量',
  release_count INT(11) COMMENT '待释放量',
  PRIMARY KEY(stat_date, app_key, clnt)
) COMMENT='黑名单分析';

-- 广告下发 展示 点击 安装 关闭 卸载
-- hive
CREATE TABLE IF NOT EXISTS mid_ad (
  adcode VARCHAR(100) COMMENT '广告CODE',
  udid VARCHAR(50) COMMENT '设备ID',
  adkey VARCHAR(100) COMMENT '广告ID',
  adverid BIGINT COMMENT '广告主ID',
  position BIGINT COMMENT '广告位',
  app_key VARCHAR(50) COMMENT '产品key',
  clnt VARCHAR(20) COMMENT '渠道号',
  city_id BIGINT COMMENT '地区ID',
  send_time TIMESTAMP COMMENT '下发时间',
  show_time TIMESTAMP COMMENT '展现时间',
  click_time TIMESTAMP COMMENT '点击时间',
  install_time TIMESTAMP COMMENT '安装时间',
  close_time TIMESTAMP COMMENT '关闭时间',
  uninstall_time TIMESTAMP COMMENT '卸载时间'
) COMMENT '广告下发反馈' PARTITIONED BY (stat_date STRING) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS fact_ad (
  adcode VARCHAR(100) COMMENT '广告CODE',
  udid VARCHAR(50) COMMENT '设备ID',
  adkey VARCHAR(100) COMMENT '广告ID',
  position BIGINT(20) COMMENT '广告位',
  app_key VARCHAR(50) COMMENT '产品key',
  clnt VARCHAR(20) COMMENT '渠道号',
  city_id BIGINT(20) COMMENT '地区ID',
  send_date INT(11) COMMENT '下发日期',
  send_hour TINYINT(4) COMMENT '下发时刻',
  show_date INT(11) COMMENT '展现日期',
  show_hour TINYINT(4) COMMENT '展现时刻',
  click_date INT(11) COMMENT '点击日期',
  click_hour TINYINT(4) COMMENT '点击时刻',
  install_date INT(11) COMMENT '安装日期',
  install_hour TINYINT(4) COMMENT '安装时刻',
  close_date INT(11) COMMENT '关闭日期',
  close_hour TINYINT(4) COMMENT '关闭时刻',
  uninstall_date INT(11) COMMENT '卸载日期',
  uninstall_hour TINYINT(4) COMMENT '卸载时刻',
  PRIMARY KEY (adcode, udid)
) COMMENT='广告下发反馈';
