-- fact_real_user
INSERT IGNORE INTO base_dw.fact_real_user (customer_id, apk_version, create_date, create_hour, user_count) 
SELECT clt, vi, DATE(createtime), HOUR(createtime), COUNT(1) 
FROM device 
WHERE createtime >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 HOUR 
AND createtime < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY clt, vi, DATE(createtime), HOUR(createtime);


-- full_active_user
INSERT IGNORE INTO full_active_user (uuid,customer_id,apk_version,active_date,create_date,first_apk_version,visit_count) 
SELECT a.uuid, a.customid, a.apkversioncode, DATE(a.createtime), DATE(b.createtime), b.vi, COUNT(1) 
FROM visitlog a 
INNER JOIN device b 
ON a.uuid = b.uid 
AND a.createtime >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND a.createtime < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY a.uuid, DATE(a.createtime) 
ORDER BY a.createtime;


-- fact_active_user
REPLACE INTO base_dw.fact_active_user (customer_id,apk_version,create_date,visit_count,user_count) 
SELECT customer_id, apk_version, active_date, visit_count, COUNT(1) 
FROM full_active_user 
WHERE active_date >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND active_date < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY customer_id, apk_version, active_date, visit_count;


-- fact_keep_user
INSERT IGNORE INTO base_dw.fact_keep_user (customer_id,apk_version,create_date,day_diff,keep_user_count) 
SELECT a.customer_id, b.vi, DATE(b.createtime), DATEDIFF(a.active_date, b.createtime), COUNT(1) 
FROM full_active_user a 
INNER JOIN 
device b 
ON a.uuid = b.uid 
AND a.active_date >= STR_TO_DATE('#run_time#','%Y%m%d') - INTERVAL 1 DAY 
AND a.active_date < STR_TO_DATE('#run_time#','%Y%m%d') 
GROUP BY a.customer_id, b.vi, DATE(b.createtime), DATEDIFF(a.active_date, b.createtime);


-- fact_user 先将数据存储到临时表，然后更新到总表
CREATE TEMPORARY TABLE IF NOT EXISTS base_dw.tmp_fact_user LIKE base_dw.fact_user;
-- fact_user 新增用户
INSERT INTO base_dw.tmp_fact_user (customer_id, apk_version, create_date, new_user_count) 
SELECT clt, vi, DATE(createtime), COUNT(1) 
FROM device 
WHERE createtime >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND createtime < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY clt, vi, DATE(createtime);
-- fact_user 访问用户
CREATE TEMPORARY TABLE IF NOT EXISTS base_dw.tmp_active_user AS 
SELECT customer_id, first_apk_version, active_date, COUNT(1) visit_user_cnt 
FROM full_active_user 
WHERE active_date >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND active_date < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY customer_id, first_apk_version, active_date;
INSERT INTO base_dw.tmp_fact_user (customer_id, apk_version, create_date, visit_user_count) 
SELECT customer_id, first_apk_version, active_date, visit_user_cnt 
FROM base_dw.tmp_active_user 
ON DUPLICATE KEY UPDATE visit_user_count = visit_user_cnt;
-- fact_user 活跃用户
UPDATE base_dw.tmp_fact_user SET active_user_count = visit_user_count - new_user_count;
-- fact_user 总用户
CREATE TEMPORARY TABLE IF NOT EXISTS base_dw.tmp_user AS 
SELECT clt customer_id, vi apk_version, #prev_day# create_date, COUNT(1) total_user_cnt 
FROM device 
WHERE createtime < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY customer_id, apk_version, create_date;
INSERT INTO base_dw.tmp_fact_user (customer_id, apk_version, create_date, total_user_count) 
SELECT customer_id, apk_version, create_date, total_user_cnt 
FROM base_dw.tmp_user 
ON DUPLICATE KEY UPDATE total_user_count = total_user_cnt;
-- 更新到fact_user表
REPLACE INTO base_dw.fact_user 
(customer_id, apk_version, create_date, new_user_count, visit_user_count, active_user_count, total_user_count) 
SELECT customer_id, apk_version, create_date, new_user_count, visit_user_count, active_user_count, total_user_count 
FROM base_dw.tmp_fact_user;


-- full_base_station
INSERT IGNORE INTO full_base_station (uuid,customer_id,station_code,create_date) 
SELECT uuid, customer_id, station_code, DATE(log_time) 
FROM log_base_station 
WHERE create_time >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND create_time < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY uuid, customer_id, station_code, DATE(log_time);


-- full_locked
INSERT IGNORE INTO full_locked (uuid,customer_id,create_date,unlock_count) 
SELECT uuid, customer_id, DATE(unlock_log_date), SUM(unlock_cnt) 
FROM log_locked 
WHERE create_time >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND create_time < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY uuid, customer_id, DATE(unlock_log_date);


-- full_app_used
INSERT IGNORE INTO full_app_used (uuid,customer_id,create_date,app_used_count) 
SELECT uuid, customer_id, DATE(app_used_date), SUM(open_cnt) 
FROM log_app_used 
WHERE create_time >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND create_time < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
AND pkg_name IN ('com.android.phone', 'com.android.mms', 'com.android.gallery') 
GROUP BY uuid, customer_id, DATE(app_used_date);


-- full_app
INSERT IGNORE INTO full_app (uuid,customer_id,create_date,app_install_count) 
SELECT uuid, customer_id, DATE(app_log_date), COUNT(1) 
FROM log_app 
WHERE create_time >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND create_time < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY uuid, customer_id, DATE(app_log_date);


-- full_app_account
INSERT IGNORE INTO full_app_account (uuid,customer_id,acc_name,acc_type,create_date) 
SELECT uuid, customer_id, acc_name, acc_type, DATE(create_time) 
FROM log_app_account 
WHERE create_time >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND create_time < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY uuid, customer_id, acc_name, acc_type, DATE(create_time);


-- full_network_type
INSERT IGNORE INTO full_network_type (uuid,customer_id,link_type,create_date) 
SELECT uuid, customid, linktype, DATE(createtime) 
FROM visitlog 
WHERE createtime >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND createtime < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY uuid, linktype, DATE(createtime);


-- 打分 MAC
INSERT IGNORE INTO user_score_log (uuid, rule_id, create_date) 
SELECT uuid,
IF(changed_mac = 1, #RULE_MAC_1#, #RULE_MAC_2#),
STR_TO_DATE('#run_time#','%Y%m%d') 
FROM full_other_info;
-- 打分 VPN
INSERT IGNORE INTO user_score_log (uuid, rule_id, create_date) 
SELECT uuid,
IF(used_vpn = 1, #RULE_VPN_1#, #RULE_VPN_2#),
STR_TO_DATE('#run_time#','%Y%m%d') 
FROM full_other_info;
-- 打分 手机运行时间
INSERT IGNORE INTO user_score_log (uuid, rule_id, create_date) 
SELECT uuid,
CASE WHEN uptime/1000/60/60/24 < 1 THEN #RULE_UPTIME_1# 
WHEN uptime/1000/60/60/24 > 3 THEN #RULE_UPTIME_2# 
ELSE #RULE_UPTIME_3# END,
STR_TO_DATE('#run_time#','%Y%m%d') 
FROM full_other_info;
-- 打分 手机号码
INSERT IGNORE INTO user_score_log (uuid, rule_id, create_date) 
SELECT uuid,
IF(has_phonenum = 1, #RULE_PHONENUM_1#, #RULE_PHONENUM_2#),
STR_TO_DATE('#run_time#','%Y%m%d') 
FROM full_other_info;
-- 打分 电量
INSERT IGNORE INTO user_score_log (uuid, rule_id, create_date) 
SELECT uuid,
CASE 
WHEN COUNT(DISTINCT power) = 0 THEN #RULE_POWER_1# 
WHEN COUNT(DISTINCT power) = 1 THEN #RULE_POWER_2# 
WHEN COUNT(DISTINCT power) < 5 THEN #RULE_POWER_3# 
WHEN COUNT(DISTINCT power) >= 5 THEN #RULE_POWER_4# 
END,
STR_TO_DATE('#run_time#','%Y%m%d') 
FROM full_power 
WHERE power > 0 
AND create_date >= STR_TO_DATE('#run_time#','%Y%m%d') - INTERVAL #INTERVAL_DAYS# DAY 
AND create_date < STR_TO_DATE('#run_time#','%Y%m%d') 
GROUP BY uuid;
-- 打分 基站
INSERT IGNORE INTO user_score_log (uuid, rule_id, create_date) 
SELECT uuid, 
CASE 
WHEN COUNT(DISTINCT station_code) = 0 THEN #RULE_BASESTATION_1# 
WHEN COUNT(DISTINCT station_code) = 1 THEN #RULE_BASESTATION_2# 
WHEN COUNT(DISTINCT station_code) < 5 THEN #RULE_BASESTATION_3# 
WHEN COUNT(DISTINCT station_code) >= 5 THEN #RULE_BASESTATION_4# 
END,
STR_TO_DATE('#run_time#','%Y%m%d') 
FROM full_base_station 
WHERE create_date >= STR_TO_DATE('#run_time#','%Y%m%d') - INTERVAL #INTERVAL_DAYS# DAY 
AND create_date < STR_TO_DATE('#run_time#','%Y%m%d') 
GROUP BY uuid;
-- 打分 亮屏
INSERT IGNORE INTO user_score_log (uuid, rule_id, create_date) 
SELECT uuid,
CASE 
WHEN COUNT(DISTINCT unlock_count) = 0 THEN #RULE_LOCKED_1# 
WHEN COUNT(DISTINCT unlock_count) = 1 THEN #RULE_LOCKED_2# 
WHEN COUNT(DISTINCT unlock_count) < 5 THEN #RULE_LOCKED_3# 
WHEN COUNT(DISTINCT unlock_count) >= 5 THEN #RULE_LOCKED_4# 
END,
STR_TO_DATE('#run_time#','%Y%m%d') 
FROM full_locked 
WHERE create_date >= STR_TO_DATE('#run_time#','%Y%m%d') - INTERVAL #INTERVAL_DAYS# DAY 
AND create_date < STR_TO_DATE('#run_time#','%Y%m%d') 
GROUP BY uuid;
-- 打分 APP使用
INSERT IGNORE INTO user_score_log (uuid, rule_id, create_date) 
SELECT uuid,
CASE 
WHEN SUM(app_used_count) = 0 THEN #RULE_APPUSED_1# 
WHEN SUM(app_used_count) < 10 THEN #RULE_APPUSED_2# 
WHEN SUM(app_used_count) >= 10 THEN #RULE_APPUSED_3# 
END,
STR_TO_DATE('#run_time#','%Y%m%d') 
FROM full_app_used 
WHERE create_date >= STR_TO_DATE('#run_time#','%Y%m%d') - INTERVAL #INTERVAL_DAYS# DAY 
AND create_date < STR_TO_DATE('#run_time#','%Y%m%d') 
GROUP BY uuid;
-- 打分 APP安装卸载
INSERT IGNORE INTO user_score_log (uuid, rule_id, create_date) 
SELECT uuid,
IF(SUM(app_install_count) > 3, #RULE_APPINSTALL_1#, #RULE_APPINSTALL_2#),
STR_TO_DATE('#run_time#','%Y%m%d') 
FROM full_app 
WHERE create_date >= STR_TO_DATE('#run_time#','%Y%m%d') - INTERVAL #INTERVAL_DAYS# DAY 
AND create_date < STR_TO_DATE('#run_time#','%Y%m%d') 
GROUP BY uuid;
-- 打分 应用账号
INSERT IGNORE INTO user_score_log (uuid, rule_id, create_date) 
SELECT uuid,
IF(COUNT(CONCAT(acc_name, acc_type)) > 0, #RULE_APPACCOUNT_1#, #RULE_APPACCOUNT_2#),
STR_TO_DATE('#run_time#','%Y%m%d') 
FROM full_app_account 
WHERE create_date >= STR_TO_DATE('#run_time#','%Y%m%d') - INTERVAL #INTERVAL_DAYS# DAY 
AND create_date < STR_TO_DATE('#run_time#','%Y%m%d') 
GROUP BY uuid;
-- 打分 网络类型
INSERT IGNORE INTO user_score_log (uuid, rule_id, create_date) 
SELECT uuid,
IF(COUNT(DISTINCT link_type) > 2, #RULE_NETWORKTYPE_1#, #RULE_NETWORKTYPE_2#),
STR_TO_DATE('#run_time#','%Y%m%d') 
FROM full_network_type 
WHERE create_date >= STR_TO_DATE('#run_time#','%Y%m%d') - INTERVAL #INTERVAL_DAYS# DAY 
AND create_date < STR_TO_DATE('#run_time#','%Y%m%d') 
GROUP BY uuid;
-- 打分 活跃度
INSERT IGNORE INTO user_score_log (uuid, rule_id, create_date) 
SELECT uuid,
CASE 
WHEN COUNT(1) = 1 THEN #RULE_ACTIVE_1# 
WHEN COUNT(1) > 3 THEN #RULE_ACTIVE_2# 
ELSE #RULE_ACTIVE_3# 
END,
STR_TO_DATE('#run_time#','%Y%m%d') 
FROM full_active_user 
WHERE create_date >= STR_TO_DATE('#run_time#','%Y%m%d') - INTERVAL #INTERVAL_DAYS# DAY 
AND create_date < STR_TO_DATE('#run_time#','%Y%m%d') 
GROUP BY uuid;
-- 打分 UUID状态
INSERT IGNORE INTO user_score_log (uuid, rule_id, create_date) 
SELECT uid,
CASE 
WHEN IF(changedstatus > '', changedstatus, originalstatus) = 'B' THEN #RULE_UUID_1# 
WHEN IF(changedstatus > '', changedstatus, originalstatus) = 'C' THEN #RULE_UUID_2# 
WHEN IF(changedstatus > '', changedstatus, originalstatus) = 'G' THEN #RULE_UUID_3# 
ELSE #RULE_UUID_4# END,
STR_TO_DATE('#run_time#','%Y%m%d') 
FROM device;
-- 打分 汇总
INSERT IGNORE INTO user_score (uuid, create_date, score, rule_count) 
SELECT a.uuid, a.create_date, SUM(b.score), COUNT(1) 
FROM user_score_log a 
INNER JOIN integral_rule b 
ON a.rule_id = b.id 
AND a.create_date = STR_TO_DATE('#run_time#','%Y%m%d') 
GROUP BY a.uuid, a.create_date;


-- fact_version
REPLACE INTO base_dw.fact_version (customer_id, apk_version, create_date, initial_user_count, upgrade_user_count, total_user_count) 
SELECT
  clt, 
  vi,
  DATE(createtime),
  SUM(IF(vi = updatevi OR updatevi = 0 OR updatevi IS NULL, 1, 0)),
  SUM(IF(vi <> updatevi AND updatevi > 0, 1, 0)),
  COUNT(1) 
FROM device
WHERE createtime >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND createtime < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY clt, vi, DATE(createtime);
