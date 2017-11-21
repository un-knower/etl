-- 实时新增用户
CREATE TABLE IF NOT EXISTS base_dw.fact_real_user (
  customer_id varchar(50) COMMENT '渠道编号',
  apk_version int(11) COMMENT 'APK版本',
  create_date date COMMENT '创建日期',
  create_hour tinyint COMMENT '创建小时',
  user_count int COMMENT '用户数',
  PRIMARY KEY (customer_id, apk_version, create_date, create_hour)
) COMMENT '实时新增用户';

-- 全量活跃用户
CREATE TABLE IF NOT EXISTS base_ods.full_active_user (
  uuid varchar(50) NOT NULL COMMENT '设备唯一ID',
  customer_id varchar(50) COMMENT '渠道编号',
  apk_version int(11) COMMENT 'apk版本',
  active_date date COMMENT '活跃日期',
  create_date date COMMENT '创建日期',
  first_apk_version int(11) COMMENT '初始apk版本',
  visit_count int COMMENT '访问次数',
  PRIMARY KEY (uuid, active_date)
) COMMENT '全量活跃用户';

-- 活跃用户事实表
CREATE TABLE IF NOT EXISTS base_dw.fact_active (
  uuid varchar(50) NOT NULL COMMENT '设备唯一ID',
  customer_id varchar(50) COMMENT '渠道编号',
  app_version int(11) COMMENT '当前APP版本',
  visit_date date COMMENT '访问日期',
  init_app_version int(11) COMMENT '初始APP版本',
  install_date date COMMENT '安装日期',
  day_diff int COMMENT '访问安装日期相隔天数',
  visit_times int COMMENT '访问次数',
  PRIMARY KEY (uuid, visit_date)
) COMMENT '基线服务-活跃用户事实表';

-- 活跃用户
CREATE TABLE IF NOT EXISTS base_dw.fact_active_user (
  customer_id varchar(50) COMMENT '渠道编号',
  apk_version int(11) COMMENT 'APK版本',
  create_date date COMMENT '创建日期',
  visit_count int COMMENT '访问次数区间',
  user_count int COMMENT '用户数',
  PRIMARY KEY (customer_id, apk_version, create_date, visit_count)
) COMMENT '活跃用户';

-- 留存用户
CREATE TABLE IF NOT EXISTS base_dw.fact_keep_user (
  customer_id varchar(50) COMMENT '渠道编号',
  apk_version int(11) COMMENT 'APK版本',
  create_date date COMMENT '创建日期',
  day_diff int COMMENT '留存区间',
  keep_user_count int COMMENT '留存用户数',
  PRIMARY KEY (customer_id, apk_version, create_date, day_diff)
) COMMENT '留存用户';

CREATE TABLE IF NOT EXISTS base_ods.full_other_info (
  uuid varchar(32) COMMENT '设备唯一标识',
  customer_id varchar(20) COMMENT '渠道编号',
  used_vpn int(11) COMMENT 'wifi的代理1使用 0没有使用',
  uptime bigint(20) COMMENT '手机运行总时长',
  changed_mac int(1) COMMENT 'MAC地址是否一致 1一致 0不一致',
  has_phonenum varchar(13) COMMENT '手机号码',
  create_date date COMMENT '创建日期',
  update_date date COMMENT '更新日期',
  PRIMARY KEY (uuid)
) COMMENT '全量设备其他信息';

CREATE TABLE IF NOT EXISTS base_dw.fact_other_info (
  customer_id varchar(20) COMMENT '渠道编号',
  create_date date COMMENT '创建日期',
  used_vpn int(1) COMMENT 'wifi的代理1使用 0没有使用',
  changed_mac int(1) COMMENT 'MAC地址是否一致 1一致 0不一致',
  uptime bigint COMMENT '手机运行总时长',
  has_phonenum int(1) COMMENT '是否有手机号码',
  user_count int(11) COMMENT '用户数'
);

CREATE TABLE IF NOT EXISTS base_dw.fact_base_station (
  uuid varchar(50) COMMENT '设备唯一标识',
  customer_id varchar(50) COMMENT '渠道编号',
  base_station_count int NOT NULL DEFAULT 0 COMMENT '基站个数',
  stat_date date COMMENT '统计日期',
  install_date date COMMENT '安装日期',
  PRIMARY KEY (uuid)
);

CREATE TABLE IF NOT EXISTS base_dw.fact_app (
  uuid varchar(50) COMMENT '设备唯一标识',
  customer_id varchar(50) COMMENT '渠道编号',
  install_count int DEFAULT 0 COMMENT '安装卸载次数',
  stat_date date COMMENT '统计日期',
  install_date date COMMENT '安装日期',
  PRIMARY KEY (uuid)
);

CREATE TABLE IF NOT EXISTS base_dw.fact_app_account (
  uuid varchar(50) COMMENT '设备唯一标识',
  customer_id varchar(50) COMMENT '渠道编号',
  app_account_count int NOT NULL DEFAULT 0 COMMENT '应用账号个数',
  stat_date date COMMENT '统计日期',
  install_date date COMMENT '安装日期',
  PRIMARY KEY (uuid)
);

CREATE TABLE IF NOT EXISTS base_dw.fact_app_used (
  uuid varchar(50) COMMENT '设备唯一标识',
  customer_id varchar(50) COMMENT '渠道编号',
  app_used_times int NOT NULL DEFAULT 0 COMMENT 'APP使用次数',
  stat_date date COMMENT '统计日期',
  install_date date COMMENT '安装日期',
  PRIMARY KEY (uuid)
);

CREATE TABLE IF NOT EXISTS base_dw.fact_locked (
  uuid varchar(50) COMMENT '设备唯一标识',
  customer_id varchar(50) COMMENT '渠道编号',
  unlock_count int NOT NULL DEFAULT 0 COMMENT '亮屏次数',
  stat_date date COMMENT '统计日期',
  install_date date COMMENT '安装日期',
  PRIMARY KEY (uuid)
);

CREATE TABLE IF NOT EXISTS base_dw.fact_power (
  uuid varchar(50) COMMENT '设备唯一标识',
  customer_id varchar(50) COMMENT '渠道编号',
  power_count int NOT NULL DEFAULT 0 COMMENT '电量个数',
  stat_date date COMMENT '统计日期',
  install_date date COMMENT '安装日期',
  PRIMARY KEY (uuid)
);

CREATE TABLE IF NOT EXISTS base_dw.fact_network (
  uuid varchar(50) COMMENT '设备唯一标识',
  customer_id varchar(50) COMMENT '渠道编号',
  link_type varchar(20) COMMENT '最高联网方式',
  link_type_count int NOT NULL DEFAULT 0 COMMENT '联网方式个数',
  stat_date date COMMENT '统计日期',
  install_date date COMMENT '安装日期',
  PRIMARY KEY (uuid)
);

CREATE TABLE IF NOT EXISTS base_dw.fact_user (
  customer_id varchar(50) COMMENT '渠道编号',
  apk_version int(11) COMMENT 'APK版本',
  create_date date COMMENT '创建日期',
  new_user_count int COMMENT '新增用户数',
  visit_user_count int COMMENT '访问用户数',
  active_user_count int COMMENT '活跃用户数',
  total_user_count int COMMENT '总用户数',
  PRIMARY KEY (customer_id, apk_version, create_date)
) COMMENT '用户';

CREATE TABLE IF NOT EXISTS base_dw.fact_version (
  customer_id varchar(50) COMMENT '渠道编号',
  apk_version int(11) COMMENT 'APK版本',
  create_date date COMMENT '创建日期',
  initial_user_count int(1) COMMENT '初始用户数',
  upgrade_user_count int(11) COMMENT '升级用户数',
  total_user_count int(11) COMMENT '总用户数',
  PRIMARY KEY (customer_id, apk_version, create_date)
) COMMENT 'apk版本';


CREATE TABLE IF NOT EXISTS base_ods.integral_rule (
  id int COMMENT '主键',
  dim_type varchar(32) COMMENT '维度类型',
  rule_code varchar(64) COMMENT '规则编码',
  rule_desc varchar(255) COMMENT '规则描述',
  time_range varchar(32) COMMENT '时间范围',
  score int NOT NULL DEFAULT 0 COMMENT '分数',
  valid tinyint NOT NULL DEFAULT 0 COMMENT '规则是否有效 0:有效 1:无效',
  remark varchar(255) COMMENT '备注',
  PRIMARY KEY (id)
) COMMENT '积分规则';

INSERT INTO base_ods.integral_rule (id, dim_type, rule_code, rule_desc, time_range, score, remark) VALUES 
('1', 'MAC地址', 'RULE_MAC_1', 'MAC地址一致', '历史所有', '5', '是否有模拟MAC行为'),
('2', 'MAC地址', 'RULE_MAC_2', 'MAC地址不一致', '历史所有', '-20', null),
('3', 'VPN代理', 'RULE_VPN_1', '使用过VPN代理', '历史所有', '-10', '是否使用代理IP行为'),
('4', 'VPN代理', 'RULE_VPN_2', '没有使用过VPN代理', '历史所有', '5', null),
('5', '手机运行时间', 'RULE_UPTIME_1', '手机运行时间小于1天', '最后一次', '-5', '最近开机到现在的时长'),
('6', '手机运行时间', 'RULE_UPTIME_2', '手机运行时间大于3天', '最后一次', '5', null),
('7', '手机运行时间', 'RULE_UPTIME_3', '其他情况', '最后一次', '0', null),
('8', '手机号码', 'RULE_PHONENUM_1', '有手机号', '历史所有', '5', '移动卡是否能拿到手机号码'),
('9', '手机号码', 'RULE_PHONENUM_2', '没有手机号', '历史所有', '0', null),
('10', '最低电量', 'RULE_POWER_1', '最低电量没有值', '10天之内', '-10', '每天的最低电量是否有变化'),
('11', '最低电量', 'RULE_POWER_2', '最低电量只有一个值', '10天之内', '-5', null),
('12', '最低电量', 'RULE_POWER_3', '变化次数小于5', '10天之内', '5', null),
('13', '最低电量', 'RULE_POWER_4', '变化次数大于等于5', '10天之内', '10', null),
('14', '基站信息', 'RULE_BASESTATION_1', '基站信息没有值', '10天之内', '-10', '联网的基站信息是否有变化'),
('15', '基站信息', 'RULE_BASESTATION_2', '基站个数为1', '10天之内', '-5', null),
('16', '基站信息', 'RULE_BASESTATION_3', '基站个数大于1，小于5', '10天之内', '5', null),
('17', '基站信息', 'RULE_BASESTATION_4', '基站个数大于等于5', '10天之内', '10', null),
('18', '亮屏黑屏', 'RULE_LOCKED_1', '亮屏黑屏没有值', '10天之内', '-10', '每天黑屏亮屏次数是否有变化'),
('19', '亮屏黑屏', 'RULE_LOCKED_2', '亮屏黑屏只有一个值', '10天之内', '-10', null),
('20', '亮屏黑屏', 'RULE_LOCKED_3', '变化次数小于5', '10天之内', '5', null),
('21', '亮屏黑屏', 'RULE_LOCKED_4', '变化次数大于等于5', '10天之内', '10', null),
('22', 'APP使用', 'RULE_APPUSED_1', '没有使用电话、短信、相机之一', '10天之内', '-10', '手机常用功能用到频率'),
('23', 'APP使用', 'RULE_APPUSED_2', '使用次数为1-10', '10天之内', '5', null),
('24', 'APP使用', 'RULE_APPUSED_3', '使用次数大于10', '10天之内', '10', null),
('25', '网络类型', 'RULE_NETWORKTYPE_1', '网络类型个数大于2', '10天之内', '5', 'wifi,2g,3g,4g是否有切换'),
('26', '网络类型', 'RULE_NETWORKTYPE_2', '其他情况', '10天之内', '0', null),
('27', '安装卸载', 'RULE_APPINSTALL_1', '安装卸载次数大于3', '10天之内', '5', '是否有卸载安装应用行为'),
('28', '安装卸载', 'RULE_APPINSTALL_2', '其他情况', '10天之内', '0', null),
('29', '应用账号', 'RULE_APPACCOUNT_1', '应用账号个数大于等于1', '历史所有', '5', '是否有微信，支付宝等账号信息'),
('30', '应用账号', 'RULE_APPACCOUNT_2', '其他情况', '历史所有', '0', null),
('31', '活跃度', 'RULE_ACTIVE_1', '活跃天数等于1', '10天之内', '-5', null),
('32', '活跃度', 'RULE_ACTIVE_2', '活跃天数大于3', '10天之内', '5', '活跃情况'),
('33', '活跃度', 'RULE_ACTIVE_3', '其他情况', '10天之内', '0', null);

CREATE TABLE IF NOT EXISTS base_ods.user_score_log (
  uuid varchar(32) COMMENT '设备唯一标识',
  rule_id int COMMENT '规则ID',
  create_date date COMMENT '创建日期',
  PRIMARY KEY (uuid, rule_id, create_date)
) COMMENT '评分记录';

CREATE TABLE IF NOT EXISTS base_ods.user_score (
  uuid varchar(32) COMMENT '设备唯一标识',
  create_date date COMMENT '创建日期',
  score int COMMENT '分数',
  rule_count int(11) DEFAULT NULL COMMENT '规则条数',
  PRIMARY KEY (uuid, create_date)
) COMMENT '用户分数';

CREATE TABLE IF NOT EXISTS fact_device (
  uuid varchar(50) COMMENT '设备唯一ID',
  is_repeated tinyint(4) DEFAULT '0' COMMENT '是否是重复的量 0否 1是',
  create_time datetime COMMENT '创建时间',
  update_time datetime COMMENT '修改时间',
  customer_id varchar(50) COMMENT '渠道编号',
  init_app_version int(11) COMMENT '初始APP版本',
  app_version int(11) DEFAULT '0' COMMENT '当前APP版本',
  original_status varchar(1) COMMENT '原始状态',
  changed_status varchar(1) COMMENT '更改后的状态',
  android_id varchar(50) COMMENT 'Android ID',
  imsi varchar(50) COMMENT 'IMSI',
  install_date date COMMENT '安装日期',
  isp_code int(11) COMMENT '运营商编码',
  aid_status tinyint(4) DEFAULT '0' COMMENT 'Android ID是否重复 0否 1是',
  imsi_status tinyint(4) DEFAULT '0' COMMENT 'IMSI 是否重复 0否 1是',
  is_upgraded tinyint(4) DEFAULT '0' COMMENT 'APP是否升级 0否 1是',
  is_activated tinyint(4) DEFAULT '0' COMMENT '是否激活 0否 1是',
  activate_date date COMMENT '激活日期',
  first_score int(3) COMMENT '首次有效得分',
  first_score_date date COMMENT '首次有效打分日期',
  last_score int(3) COMMENT '最近一次得分',
  last_score_date date COMMENT '最近一次打分日期',
  PRIMARY KEY (uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='基线服务-设备事实表';
