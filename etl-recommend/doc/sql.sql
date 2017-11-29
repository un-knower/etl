-- 推荐系统数据库
CREATE TABLE IF NOT EXISTS keyword_freq (
  stat_date INT COMMENT '统计日期',
  keyword VARCHAR(64) COMMENT '关键词',
  frequency INT COMMENT '频率',
  PRIMARY KEY (stat_date, keyword)
) ENGINE=InnoDB COMMENT='关键词频率';

CREATE TABLE IF NOT EXISTS user_profile (
  stat_date INT COMMENT '统计日期',
  uuid VARCHAR(50) COMMENT '用户ID',
  keywords VARCHAR(2000) COMMENT '用户兴趣',
  PRIMARY KEY (stat_date, uuid)
) ENGINE=InnoDB COMMENT='用户模型';

CREATE TABLE IF NOT EXISTS user_recommend(
  uuid VARCHAR(50) COMMENT '用户ID',
  info_id BIGINT COMMENT '资讯ID',
  similarity FLOAT(10,5) COMMENT '相似度',
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '推荐时间',
  PRIMARY KEY (uuid, info_id)
) ENGINE=InnoDB COMMENT='推荐结果';


-- 业务数据库
CREATE TABLE IF NOT EXISTS info_profile (
  id BIGINT COMMENT '主键ID',
  keywords VARCHAR(255) COMMENT '资讯关键词',
  source VARCHAR(64) COMMENT '来源',
  region VARCHAR(64) COMMENT '地域',
  PRIMARY KEY (id)
) ENGINE=InnoDB COMMENT='资讯模型'

CREATE TABLE IF NOT EXISTS t_information (
  id BIGINT COMMENT '主键ID',
  category_name VARCHAR(128) COMMENT '类别名称',
  title VARCHAR(128) COMMENT '标题',
  content TEXT COMMENT '内容',
  introduction VARCHAR(255) COMMENT '摘要',
  source VARCHAR(64) COMMENT '来源',
  update_time DATETIME COMMENT '修改时间',
  PRIMARY KEY (id)
) ENGINE=InnoDB COMMENT='资讯'



-- hive
USE recommender;
CREATE TABLE IF NOT EXISTS d_user_click (
  device_id VARCHAR(50) COMMENT '设备ID',
  info_id BIGINT COMMENT '资讯ID',
  event_name VARCHAR(50) COMMENT '事件名称',
  click_count INT COMMENT '点击次数'
) COMMENT '用户点击资讯次数-日汇总' PARTITIONED BY (click_date STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS d_info_click (
  info_id BIGINT COMMENT '资讯ID',
  user_count INT COMMENT '点击人数'
) COMMENT '资讯点击人数-日汇总' PARTITIONED BY (click_date STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS d_user_profile (
  device_id VARCHAR(50) COMMENT '设备ID',
  tags VARCHAR(255) COMMENT '标签',
  wtags VARCHAR(255) COMMENT '权重标签'
) COMMENT '用户兴趣模型' PARTITIONED BY (stat_date STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;


SELECT b.device_id, b.event_name, b.info_id, DATEDIFF('2016-10-26}', b.click_date) date_diff, b.click_count, a.user_count
FROM (SELECT * FROM d_info_click WHERE click_date >= '2016-10-01' AND click_date <= '2016-10-25}') a
JOIN (SELECT * FROM d_user_click WHERE click_date >= '2016-10-01' AND click_date <= '2016-10-25}') b
ON a.click_date = b.click_date
AND a.info_id = b.info_id;