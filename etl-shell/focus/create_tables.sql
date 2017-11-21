-- visitlog
CREATE TABLE IF NOT EXISTS focus_ods.visitlog (
  uuid varchar(50) NOT NULL COMMENT '设备唯一ID',
  username varchar(50) COMMENT '注册用户名',
  customer_id varchar(50) COMMENT '渠道编号',
  app_version int(11) COMMENT 'APP版本',
  os_type tinyint(4) COMMENT '操作系统类型 0:android 1:ios',
  visit_time datetime COMMENT '访问时间'
) COMMENT '访问日志';

-- clicklog
CREATE TABLE IF NOT EXISTS focus_ods.clicklog (
  uuid varchar(50) NOT NULL COMMENT '设备唯一ID',
  username varchar(50) COMMENT '注册用户名',
  customer_id varchar(50) COMMENT '渠道编号',
  app_version int(11) COMMENT 'APP版本',
  opt_type int(11) COMMENT '操作类型',
  protocol_id int(11) COMMENT '协议号',
  opt_object varchar(32) COMMENT '操作对象',
  os_type tinyint(4) COMMENT '操作系统类型 0:android 1:ios',
  click_time datetime COMMENT '点击时间'
) COMMENT '点击日志';

-- 设备事实表
CREATE TABLE IF NOT EXISTS focus_dw.fact_device (
  uuid varchar(50) NOT NULL COMMENT '设备唯一ID',
  username varchar(50) COMMENT '注册用户名',
  customer_id varchar(50) COMMENT '渠道编号',
  init_app_version int(11) COMMENT '初始APP版本',
  app_version int(11) COMMENT '当前APP版本',
  os_type tinyint(4) COMMENT '操作系统类型 0:android 1:ios',
  activate_date date COMMENT '激活日期',
  update_date date COMMENT '更新日期',
  PRIMARY KEY (uuid)
) COMMENT '牙牙关注-设备事实表';

-- 活跃用户事实表
CREATE TABLE IF NOT EXISTS focus_dw.fact_active (
  uuid varchar(50) NOT NULL COMMENT '设备唯一ID',
  username varchar(50) COMMENT '注册用户名',
  customer_id varchar(50) COMMENT '渠道编号',
  app_version int(11) COMMENT '当前APP版本',
  visit_date date COMMENT '访问日期',
  init_app_version int(11) COMMENT '初始APP版本',
  os_type tinyint(4) COMMENT '操作系统类型 0:android 1:ios',
  activate_date date COMMENT '激活时间',
  day_diff int COMMENT '访问激活日期相隔天数',
  start_times int COMMENT '启动次数',
  PRIMARY KEY (uuid, visit_date)
) COMMENT '牙牙关注-活跃用户事实表';

-- 活跃用户
CREATE TABLE IF NOT EXISTS focus_dw.fact_active_user (
  customer_id varchar(50) COMMENT '渠道编号',
  apk_version int(11) COMMENT 'APK版本',
  os_type tinyint(4) COMMENT '操作系统类型 0:android 1:ios',
  active_date date COMMENT '活跃日期',
  date_diff int COMMENT '历史天数',
  acitve_user_count int COMMENT '活跃用户数',
  PRIMARY KEY (customer_id, apk_version, os_type, active_date, date_diff)
) COMMENT '活跃用户';

-- 留存用户
CREATE TABLE IF NOT EXISTS focus_dw.fact_keep_user (
  customer_id varchar(50) COMMENT '渠道编号',
  apk_version int(11) COMMENT '初始APK版本',
  os_type tinyint(4) COMMENT '操作系统类型 0:android 1:ios',
  create_date date COMMENT '创建日期',
  date_diff int COMMENT '留存区间',
  keep_user_count int COMMENT '留存用户数',
  PRIMARY KEY (customer_id, apk_version, os_type, create_date, date_diff)
) COMMENT '留存用户';

-- 用户跟踪
CREATE TABLE IF NOT EXISTS focus_dw.fact_user_trace (
  customer_id varchar(50) COMMENT '渠道编号',
  apk_version int(11) COMMENT '初始APK版本',
  os_type tinyint(4) COMMENT '操作系统类型 0:android 1:ios',
  create_date date COMMENT '创建日期',
  new_user_count int COMMENT '新增用户数',
  day_1_count int COMMENT '次日留存用户数',
  day_7_count int COMMENT '7日内留存用户数',
  day_14_count int COMMENT '14日内留存用户数',
  day_30_count int COMMENT '30日内留存用户数',
  day_60_count int COMMENT '60日内留存用户数',
  PRIMARY KEY (customer_id, apk_version, os_type, create_date)
) COMMENT '用户跟踪';

-- 用户
CREATE TABLE IF NOT EXISTS focus_dw.fact_user (
  customer_id varchar(50) COMMENT '渠道编号',
  apk_version int(11) COMMENT '初始APK版本',
  os_type tinyint(4) COMMENT '操作系统类型 0:android 1:ios',
  create_date date COMMENT '创建日期',
  has_username tinyint DEFAULT 0 COMMENT '是否注册，0没有注册，1有注册',
  new_user_count int COMMENT '新增用户数',
  visit_user_count int COMMENT '访问用户数',
  active_user_count int COMMENT '活跃用户数',
  total_user_count int COMMENT '累计用户数',
  PRIMARY KEY (customer_id, apk_version, os_type, create_date, has_username)
) COMMENT '用户';

-- 启动次数
CREATE TABLE IF NOT EXISTS focus_dw.fact_start (
  customer_id varchar(50) COMMENT '渠道编号',
  apk_version int(11) COMMENT 'APK版本',
  os_type tinyint(4) COMMENT '操作系统类型 0:android 1:ios',
  visit_date date COMMENT '访问日期',
  start_times int COMMENT '启动次数',
  PRIMARY KEY (customer_id, apk_version, os_type, visit_date)
) COMMENT '启动次数';

-- 使用频率
CREATE TABLE IF NOT EXISTS focus_dw.fact_usage (
  customer_id varchar(50) COMMENT '渠道编号',
  apk_version int(11) COMMENT 'APK版本',
  os_type tinyint(4) COMMENT '操作系统类型 0:android 1:ios',
  visit_date date COMMENT '访问日期',
  start_times int COMMENT '启动次数',
  user_count int COMMENT '用户数',
  PRIMARY KEY (customer_id, apk_version, os_type, visit_date, start_times)
) COMMENT '使用频率';
