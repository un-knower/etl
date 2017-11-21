-- dim_device
CREATE TABLE IF NOT EXISTS sdk_dw.dim_device (
  uuid varchar(50) COMMENT '已校验唯一ID',
  device_id varchar(50) COMMENT '设备当前唯一ID',
  network varchar(50) COMMENT '最高网络类型，Unknown<WIFI<2G<3G<4G',
  platform varchar(50) COMMENT 'ios,android',
  have_vpn int(1) NOT NULL DEFAULT '0' COMMENT 'vpn代理  1:使用，0：未使用',
  imsi varchar(50) COMMENT 'sim卡卡号',
  wifi_mac varchar(50) COMMENT 'WIFI-mac地址',
  imei varchar(50) COMMENT 'imei',
  android_id varchar(50) COMMENT 'android id',
  baseband varchar(200) COMMENT '基带版本',
  language varchar(20) COMMENT '语言',
  resolution varchar(50) COMMENT '分辨率',
  model_name varchar(50) COMMENT '品牌',
  cpu varchar(50) COMMENT 'cpu',
  device_name varchar(50) COMMENT '机型',
  os_version varchar(50) COMMENT '当前操作系统版本',
  cameras varchar(25) COMMENT '相机信息 11,00,10,01',
  sdcard_size bigint(20) COMMENT 'sd卡总容量',
  rom_size bigint(20) COMMENT 'rom总容量',
  phone_no varchar(20) COMMENT '手机号码',
  city varchar(50) COMMENT '城市',
  region varchar(50) COMMENT '地区',
  country varchar(50) COMMENT '国家',
  uuid_type int(1) COMMENT 'uuid校验类型 1：uuid,2:android,3:imsi+imei',
  isp_code int COMMENT '运营商编码',
  create_time datetime COMMENT '创建时间',
  update_time datetime COMMENT '更新时间',
  is_exists tinyint(4) NOT NULL DEFAULT '0' COMMENT 'android id是否存在于旧版sdk',
  is_registered tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否已注册',
  PRIMARY KEY (uuid)
) ENGINE=MyISAM COMMENT='设备';

-- dim_version
CREATE TABLE IF NOT EXISTS sdk_dw.dim_version (
  id varchar(10) COMMENT '版本ID',
  version_no int COMMENT '版本号',
  PRIMARY KEY (id)
) ENGINE=MyISAM COMMENT='App版本';

-- fact_client
CREATE TABLE IF NOT EXISTS sdk_dw.fact_client (
  uuid varchar(50) COMMENT '已校验唯一ID',
  app_key varchar(50) COMMENT 'yygz,ylxx',
  customer_id varchar(50) COMMENT '渠道号',
  version varchar(10) COMMENT '当前版本',
  pkg_path int(1) NOT NULL DEFAULT '0' COMMENT '安装目录 1.system目录, 2.data目录, 3.sd卡目录, 4.vendor目录',
  create_time datetime COMMENT '创建时间',
  update_time datetime COMMENT '更新时间',
  init_version varchar(10) COMMENT '初始版本',
  create_date int COMMENT '创建日期',
  is_silent tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否沉默用户 0否 1是',
  is_activated tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否激活 0否 1是',
  activate_date int COMMENT '激活日期',
  PRIMARY KEY(uuid, app_key)
) ENGINE=MyISAM COMMENT='客户端';

-- fact_active
CREATE TABLE IF NOT EXISTS sdk_dw.fact_active (
  uuid varchar(50) COMMENT '已校验唯一ID',
  app_key varchar(50) COMMENT 'yygz,ylxx',
  active_date int COMMENT '活跃日期',
  active_hours varchar(64) COMMENT '活跃时间',
  time_diff bigint COMMENT '客户端服务器时间差（秒，最新）',
  boottime bigint(20) COMMENT '进程启动时长（分，当天最大）',
  version varchar(10) COMMENT '当前版本（当天第一次活跃时的版本）',
  sdcard_size bigint(20) COMMENT 'sd卡剩余容量（最新）',
  rom_size bigint(20) COMMENT 'rom剩余容量（最新）',
  screen_on int(11) COMMENT '开屏总数，-1：获取不到（当天最大）',
  battery int(11) COMMENT '最小电量，-1：获取不到（当天最低）',
  log_type int(1) NOT NULL DEFAULT '0' COMMENT '日志类型 1：主动，2：被动',
  city varchar(50) COMMENT '城市',
  region varchar(50) COMMENT '地区',
  country varchar(50) COMMENT '国家',
  client_ip varchar(50) COMMENT '客户端ip',
  visit_times int COMMENT '访问次数',
  start_times int COMMENT '启动次数',
  customer_id varchar(50) COMMENT '渠道号',
  init_version varchar(10) COMMENT '初始版本',
  create_date int COMMENT '创建日期',
  date_diff int COMMENT '第几天活跃',
  PRIMARY KEY (uuid, app_key, active_date, log_type)
) ENGINE=MyISAM COMMENT='活跃';

-- fact_session
CREATE TABLE IF NOT EXISTS sdk_dw.fact_session (
  uuid varchar(50) COMMENT '已校验唯一ID',
  app_key varchar(50) COMMENT 'yygz,ylxx',
  session_id varchar(64) COMMENT 'session id',
  create_time datetime COMMENT '创建时间',
  version varchar(10) COMMENT '当前版本',
  duration bigint COMMENT '时长（秒）',
  customer_id varchar(50) COMMENT '渠道号',
  prev_time datetime COMMENT '上次启动时间',
  create_date int COMMENT '创建日期',
  date_diff int COMMENT '本次与上次启动日期差，首次为-1',
  PRIMARY KEY (uuid, app_key, session_id)
) ENGINE=MyISAM COMMENT='App启动';

-- dim_channel
CREATE TABLE IF NOT EXISTS sdk_dw.dim_channel (
  id VARCHAR(50),
  channel_name VARCHAR(64),
  channel_type VARCHAR(10),
  platform INT(11),
  is_active TINYINT(4),
  create_time DATETIME,
  update_time DATETIME,
  PRIMARY KEY (id)
) ENGINE=MyISAM COMMENT='渠道';

-- fact_device
CREATE TABLE IF NOT EXISTS sdk_dw.fact_device (
  uuid varchar(50) COMMENT '已校验唯一ID',
  customer_id varchar(50) COMMENT '渠道号',
  create_date int COMMENT '创建日期',
  runtime int(11) NOT NULL DEFAULT '0' COMMENT '进程运行时长（分，最大值）',
  unlock_cnt int(11) NOT NULL DEFAULT '0' COMMENT '开屏次数（最大值）',
  battery_cnt int(11) NOT NULL DEFAULT '0' COMMENT '电量个数（去重个数）',
  etime_cnt int(11) NOT NULL DEFAULT '0' COMMENT '客户端服务器异常时差次数（异常次数）',
  station_cnt int(11) NOT NULL DEFAULT '0' COMMENT '基站个数（去重个数）',
  account_cnt int(11) NOT NULL DEFAULT '0' COMMENT '应用账号个数（去重个数）',
  install_cnt int(11) NOT NULL DEFAULT '0' COMMENT '安卸App个数（去重个数）',
  appuse_cnt int(11) NOT NULL DEFAULT '0' COMMENT '使用App个数（去重个数）',
  PRIMARY KEY (uuid)
) ENGINE=MyISAM COMMENT='设备';

-- channel_sum
CREATE TABLE IF NOT EXISTS sdk_ods.channel_sum (
  channel_id VARCHAR(50) COMMENT '渠道编号',
  user_count int COMMENT '新增用户量',
  vpn_pct decimal(5,3) COMMENT 'vpn用户占比',
  phone_pct decimal(5,3) COMMENT '移动号收集率',
  network_pct decimal(5,3) COMMENT '网络类型占比（WIFI+未知）',
  runtime_pct decimal(5,3) COMMENT '进程时长占比（1分钟）',
  unlock_pct decimal(5,3) COMMENT '亮屏次数占比（未知+1次）',
  battery_pct decimal(5,3) COMMENT '电量个数占比（未知+1个）',
  etime_pct decimal(5,3) COMMENT '手机时间异常次数占比（2次以上）',
  station_pct decimal(5,3) COMMENT '基站个数占比（未知+1个）',
  account_pct decimal(5,3) COMMENT '应用账号个数占比（未知）',
  install_pct decimal(5,3) COMMENT '安卸App个数占比（未知）',
  appuse_pct decimal(5,3) COMMENT '使用App个数占比（未知）'
) ENGINE=MyISAM COMMENT='渠道汇总';

-- channel_keep
CREATE TABLE IF NOT EXISTS sdk_ods.channel_keep (
  channel_id VARCHAR(50) COMMENT '渠道编号',
  user_count int COMMENT '新增用户量',
  keep_pct_1 decimal(5,3) COMMENT '次日留存率',
  keep_pct_3 decimal(5,3) COMMENT '3日留存率',
  keep_pct_6 decimal(5,3) COMMENT '6日留存率'
) ENGINE=MyISAM COMMENT='渠道留存率';

-- fact_channel
CREATE TABLE IF NOT EXISTS sdk_dw.fact_channel (
  channel_id VARCHAR(50) COMMENT '渠道编号',
  stat_date int COMMENT '统计日期',
  channel_rank CHAR(1) COMMENT '渠道等级 A,B,C,D,E',
  user_count int COMMENT '新增用户量',
  keep_pct_1 decimal(5,3) COMMENT '次日留存率',
  keep_pct_3 decimal(5,3) COMMENT '3日留存率',
  keep_pct_6 decimal(5,3) COMMENT '6日留存率',
  vpn_pct decimal(5,3) COMMENT 'vpn用户占比',
  phone_pct decimal(5,3) COMMENT '移动号收集率',
  network_pct decimal(5,3) COMMENT '网络类型占比（WIFI+未知）',
  runtime_pct decimal(5,3) COMMENT '进程时长占比（1分钟）',
  unlock_pct decimal(5,3) COMMENT '亮屏次数占比（未知+1次）',
  battery_pct decimal(5,3) COMMENT '电量个数占比（未知+1个）',
  etime_pct decimal(5,3) COMMENT '手机时间异常次数占比（2次以上）',
  station_pct decimal(5,3) COMMENT '基站个数占比（未知+1个）',
  account_pct decimal(5,3) COMMENT '应用账号个数占比（未知）',
  install_pct decimal(5,3) COMMENT '安卸App个数占比（未知）',
  appuse_pct decimal(5,3) COMMENT '使用App个数占比（未知）',
  PRIMARY KEY (channel_id, stat_date)
) ENGINE=MyISAM COMMENT='渠道';