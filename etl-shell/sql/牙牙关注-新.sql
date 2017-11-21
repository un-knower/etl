-- 数据准备
DROP TABLE IF EXISTS focus_ods.t_user;
CREATE TABLE focus_ods.t_user (
	uid VARCHAR(50) COMMENT '用户ID(androidid + uuid)',
	android_id VARCHAR(50),
	uuid VARCHAR(50),
	channel_id VARCHAR(50) COMMENT '渠道编号',
	app_version VARCHAR(20) COMMENT 'App版本号',
	os_type INT(1) COMMENT '操作系统类型(0:Android, 1:iOS)',
	create_date DATE COMMENT '创建日期',
	PRIMARY KEY (uid)
) COMMENT='用户表';

INSERT INTO focus_ods.t_user (uid, android_id, uuid, channel_id, app_version, os_type, create_date) 
SELECT IFNULL(androidid, uuid) uid, androidid, uuid, clnt, appver, ostype, DATE(MIN(createtime)) 
FROM focus_ods.t_req_stat 
GROUP BY uid;

DROP TABLE IF EXISTS focus_ods.t_active;
CREATE TABLE focus_ods.t_active (
	uid VARCHAR(50),
	visit_date DATE COMMENT '访问日期',
	app_version VARCHAR(20) COMMENT 'App版本号',
	city_code VARCHAR(20) COMMENT '城市编号',
	ip varchar(50) COMMENT 'IP地址',
	channel_id VARCHAR(50) COMMENT '渠道编号',
	init_app_version VARCHAR(20) COMMENT '初始App版本号',
	create_date DATE COMMENT '创建日期',
	os_type INT(1) COMMENT '操作系统类型(0:Android, 1:iOS)',
	day_diff INT(11) COMMENT '访问激活日期相隔天数',
	start_times INT(11) COMMENT '启动次数',
	PRIMARY KEY (uid, visit_date)
) COMMENT '活跃表';

INSERT INTO focus_ods.t_active (uid, visit_date, app_version, city_code, ip, channel_id, init_app_version, create_date, os_type, day_diff, start_times) 
SELECT a.uid, a.visit_date, a.appver, a.citycode, a.ip, b.channel_id, b.app_version, b.create_date, b.os_type, DATEDIFF(a.visit_date, b.create_date), a.visit_times 
FROM (
	SELECT IFNULL(androidid, uuid) uid, DATE(createtime) visit_date, appver, citycode, ip, COUNT(1) visit_times 
	FROM focus_ods.t_req_stat 
	GROUP BY uid, visit_date
) a 
INNER JOIN focus_ods.t_user b 
ON a.uid = b.uid;

-- 留存率统计
SELECT create_date,
	COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) new_cnt,
	ROUND(COUNT(DISTINCT uid, IF(day_diff = 1, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) * 100, 2) keep_pct,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 1, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) * 100, 2) keep_pct_1,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 2, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) * 100, 2) keep_pct_2,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 3, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) * 100, 2) keep_pct_3,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 4, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) * 100, 2) keep_pct_4,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 5, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) * 100, 2) keep_pct_5,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 6, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) * 100, 2) keep_pct_6,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 7, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) * 100, 2) keep_pct_7,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 14, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) * 100, 2) keep_pct_14,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 30, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) * 100, 2) keep_pct_30 
FROM focus_ods.t_active 
GROUP BY create_date;

-- 活跃用户统计
SELECT visit_date,
	COUNT(DISTINCT uid) visit_cnt,
	SUM(start_times) visit_times,
	COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) new_cnt,
	COUNT(DISTINCT uid) - COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) active_cnt,
	ROUND((COUNT(DISTINCT uid) - COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL))) / COUNT(DISTINCT uid) * 100, 2) active_pct 
FROM focus_ods.t_active 
GROUP BY visit_date;

-- 渠道整体留存率统计
SELECT channel_id,
	COUNT(DISTINCT uid, IF(day_diff = 0, 1, NULL)) new_cnt,
	ROUND(COUNT(DISTINCT uid, IF(day_diff = 1, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0 AND create_date < CURDATE() - INTERVAL 1 DAY, 1, NULL)) * 100, 2) keep_pct,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 1, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0 AND create_date < CURDATE() - INTERVAL 1 DAY, 1, NULL)) * 100, 2) keep_pct_1,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 2, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0 AND create_date < CURDATE() - INTERVAL 2 DAY, 1, NULL)) * 100, 2) keep_pct_2,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 3, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0 AND create_date < CURDATE() - INTERVAL 3 DAY, 1, NULL)) * 100, 2) keep_pct_3,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 4, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0 AND create_date < CURDATE() - INTERVAL 4 DAY, 1, NULL)) * 100, 2) keep_pct_4,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 5, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0 AND create_date < CURDATE() - INTERVAL 5 DAY, 1, NULL)) * 100, 2) keep_pct_5,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 6, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0 AND create_date < CURDATE() - INTERVAL 6 DAY, 1, NULL)) * 100, 2) keep_pct_6,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 7, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0 AND create_date < CURDATE() - INTERVAL 7 DAY, 1, NULL)) * 100, 2) keep_pct_7,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 14, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0 AND create_date < CURDATE() - INTERVAL 14 DAY, 1, NULL)) * 100, 2) keep_pct_14,
	ROUND(COUNT(DISTINCT uid, IF(day_diff >= 30, 1, NULL)) / COUNT(DISTINCT uid, IF(day_diff = 0 AND create_date < CURDATE() - INTERVAL 30 DAY, 1, NULL)) * 100, 2) keep_pct_30 
FROM focus_ods.t_active 
GROUP BY channel_id;