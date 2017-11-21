-- dim_device
CREATE TABLE IF NOT EXISTS ad_dw.dim_device (
  device_id varchar(50) COMMENT '设备ID',
  custom_id varchar(50) COMMENT '客户ID',
  brand varchar(50) COMMENT '品牌',
  city_id int COMMENT '地区ID',
  imei varchar(50) COMMENT '国际移动设备识别码',
  imsi varchar(50) COMMENT '国际移动用户识别码(存储在sim卡内)',
  mac varchar(50) COMMENT 'mac地址',
  model varchar(50) COMMENT '机型',
  nettype tinyint COMMENT '网络类型，0:未知 1:wifi 2:gprs',
  rom bigint COMMENT 'rom大小',
  sdcard bigint COMMENT 'sd卡大小',
  src varchar(50) COMMENT '分辨率',
  sysver varchar(50) COMMENT '系统版本',
  version varchar(50) COMMENT '客户端版本',
  init_version varchar(50) COMMENT '客户端初始版本',
  app_path tinyint COMMENT 'app路径，0:未知 1:sys 2:data 3:sdcard 4:ven',
  is_root tinyint COMMENT '是否root，0:未知 1:root 2:非root',
  create_time datetime COMMENT '创建时间',
  update_time datetime COMMENT '更新时间',
  PRIMARY KEY (device_id)
) ENGINE=MyISAM COMMENT='设备';

-- fact_active
CREATE TABLE IF NOT EXISTS ad_dw.fact_active (
  device_id varchar(50) COMMENT '设备ID',
  active_date int COMMENT '活跃日期',
  city_id int COMMENT '地区ID',
  model varchar(50) COMMENT '机型',
  nettype tinyint COMMENT '网络类型，0:未知 1:wifi 2:gprs',
  src varchar(50) COMMENT '分辨率',
  sysver varchar(50) COMMENT '系统版本',
  version varchar(50) COMMENT '客户端版本',
  is_root tinyint COMMENT '是否root，0:未知 1:root 2:非root',
  create_date int COMMENT '创建日期',
  date_diff int COMMENT '第几天活跃',
  PRIMARY KEY (device_id, active_date)
) ENGINE=MyISAM COMMENT='活跃';

-- fact_ad_send
CREATE TABLE IF NOT EXISTS ad_dw.fact_ad_send (
  ad_id bigint COMMENT '广告ID',
  res_id bigint COMMENT '资源ID',
  device_id varchar(50) COMMENT '设备ID',
  create_date int COMMENT '创建日期',
  create_hour int COMMENT '创建时刻',
  city_id int COMMENT '地区ID',
  nettype tinyint COMMENT '网络类型，0:未知 1:wifi 2:gprs',
  version varchar(50) COMMENT '客户端版本',
  send_count int COMMENT '下发数',
  PRIMARY KEY (ad_id, device_id)
) ENGINE=MyISAM COMMENT='广告-下发';

-- fact_ad_show
CREATE TABLE IF NOT EXISTS ad_dw.fact_ad_show (
  ad_id bigint COMMENT '广告ID',
  device_id varchar(50) COMMENT '设备ID',
  send_date int COMMENT '下发日期',
  create_date int COMMENT '创建日期',
  create_hour int COMMENT '创建时刻',
  city_id int COMMENT '地区ID',
  nettype tinyint COMMENT '网络类型，0:未知 1:wifi 2:gprs',
  version varchar(50) COMMENT '客户端版本',
  show_count int COMMENT '展示数',
  PRIMARY KEY (ad_id, device_id)
) ENGINE=MyISAM COMMENT='广告-展示';

-- fact_ad_click
CREATE TABLE IF NOT EXISTS ad_dw.fact_ad_click (
  ad_id bigint COMMENT '广告ID',
  device_id varchar(50) COMMENT '设备ID',
  send_date int COMMENT '下发日期',
  create_date int COMMENT '创建日期',
  create_hour int COMMENT '创建时刻',
  city_id int COMMENT '地区ID',
  nettype tinyint COMMENT '网络类型，0:未知 1:wifi 2:gprs',
  version varchar(50) COMMENT '客户端版本',
  click_count int COMMENT '点击数',
  PRIMARY KEY (ad_id, device_id)
) ENGINE=MyISAM COMMENT='广告-点击';

-- fact_ad_install
CREATE TABLE IF NOT EXISTS ad_dw.fact_ad_install (
  ad_id bigint COMMENT '广告ID',
  device_id varchar(50) COMMENT '设备ID',
  send_date int COMMENT '下发日期',
  create_date int COMMENT '创建日期',
  create_hour int COMMENT '创建时刻',
  city_id int COMMENT '地区ID',
  nettype tinyint COMMENT '网络类型，0:未知 1:wifi 2:gprs',
  version varchar(50) COMMENT '客户端版本',
  install_count int COMMENT '安装数',
  PRIMARY KEY (ad_id, device_id)
) ENGINE=MyISAM COMMENT='广告-安装';

-- fact_op
CREATE TABLE IF NOT EXISTS ad_dw.fact_op (
  device_id varchar(50) COMMENT '设备ID',
  create_date int COMMENT '创建日期',
  run_level int COMMENT '运营过滤条件',
  PRIMARY KEY (device_id, create_date, run_level)
) ENGINE=MyISAM COMMENT='运营记录';

-- fact_apilog
CREATE TABLE IF NOT EXISTS fact_apilog (
  androidid VARCHAR(50),
  ad_id BIGINT COMMENT '广告ID',
  apiurl VARCHAR(500),
  clickurl VARCHAR(2000),
  custom_id VARCHAR(50),
  imei VARCHAR(50),
  title VARCHAR(500),
  adtype int(1) DEFAULT '2' COMMENT '1：开屏广告；2：锁屏广告；3：弹框广告；4：通知栏广告', 
  ad_status TINYINT COMMENT '0: 下发 1: 展示 2: 点击',
  create_date DATE COMMENT '下发日期',
  show_date DATE COMMENT '展示日期',
  click_date DATE COMMENT '点击日期', 
  PRIMARY KEY (androidid, ad_id)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
