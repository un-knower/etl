-- fact_active
INSERT IGNORE INTO fact_active (uuid, username, customer_id, apk_version, active_date, first_apk_version, os_type, create_date, date_diff, start_times) 
SELECT a.uuid, a.username, a.customer_id, a.apk_version, DATE(a.create_time), b.apk_version, b.os_type, b.create_date, DATEDIFF(a.create_time, b.create_date), COUNT(1) 
FROM focus_ods.visitlog a 
INNER JOIN fact_device b 
ON a.uuid = b.uuid 
AND a.create_time >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND a.create_time < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY a.uuid, DATE(a.create_time);


-- fact_keep
REPLACE INTO fact_keep (customer_id, apk_version, os_type, create_date, new_user_count, day_1_count, day_7_count, day_14_count, day_30_count, day_60_count) 
SELECT
	customer_id,
	first_apk_version,
	os_type,
	create_date,
	COUNT(IF(create_date = active_date, 1, NULL)),
	COUNT(DISTINCT uuid, IF (date_diff = 1, 1, NULL)),
	COUNT(DISTINCT uuid, IF (date_diff > 0 AND date_diff <= 7, 1, NULL)),
	COUNT(DISTINCT uuid, IF (date_diff > 0 AND date_diff <= 14, 1, NULL)),
	COUNT(DISTINCT uuid, IF (date_diff > 0 AND date_diff <= 30, 1, NULL)),
	COUNT(DISTINCT uuid, IF (date_diff > 0 AND date_diff <= 60, 1, NULL))
FROM fact_active 
WHERE active_date <= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 60 DAY 
GROUP BY customer_id, first_apk_version, os_type, create_date;


-- fact_user 先将数据存储到临时表，然后更新到总表
CREATE TEMPORARY TABLE IF NOT EXISTS focus_dw.tmp_fact_user LIKE focus_dw.fact_user;
-- fact_user 新增用户数
INSERT INTO focus_dw.tmp_fact_user (customer_id, apk_version, os_type, create_date, new_user_count) 
SELECT customer_id, apk_version, os_type, create_date, COUNT(1) 
FROM focus_ods.full_user  
WHERE create_date >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND create_date < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY customer_id, apk_version, os_type, create_date;
-- fact_user 访问用户
CREATE TEMPORARY TABLE IF NOT EXISTS focus_dw.tmp_active_user AS 
SELECT customer_id, first_apk_version, os_type, active_date, COUNT(1) visit_user_cnt 
FROM focus_ods.full_active_user 
WHERE active_date >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND active_date < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY customer_id, first_apk_version, active_date;
INSERT INTO focus_dw.tmp_fact_user (customer_id, apk_version, os_type, create_date, visit_user_count) 
SELECT customer_id, first_apk_version, os_type, active_date, visit_user_cnt 
FROM focus_dw.tmp_active_user 
ON DUPLICATE KEY UPDATE visit_user_count = visit_user_cnt;
-- fact_user 活跃用户
UPDATE focus_dw.tmp_fact_user SET active_user_count = visit_user_count - new_user_count;
-- fact_user 总用户
CREATE TEMPORARY TABLE IF NOT EXISTS focus_dw.tmp_user AS 
SELECT customer_id, apk_version, os_type, #prev_day# create_date, COUNT(1) total_user_cnt 
FROM focus_ods.full_user 
WHERE create_date < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY customer_id, apk_version, os_type, create_date;
INSERT INTO focus_dw.tmp_fact_user (customer_id, apk_version, os_type, create_date, total_user_count) 
SELECT customer_id, apk_version, os_type, create_date, total_user_cnt 
FROM focus_dw.tmp_user 
ON DUPLICATE KEY UPDATE total_user_count = total_user_cnt;
-- 更新到fact_user表
REPLACE INTO focus_dw.fact_user 
(customer_id, apk_version, os_type, create_date, new_user_count, visit_user_count, active_user_count, total_user_count) 
SELECT customer_id, apk_version, os_type, create_date, new_user_count, visit_user_count, active_user_count, total_user_count 
FROM focus_dw.tmp_fact_user;


-- fact_start
REPLACE INTO focus_dw.fact_start (customer_id, apk_version, os_type, visit_date, start_times) 
SELECT customer_id, apk_version, os_type, DATE(create_time), COUNT(1) 
FROM focus_ods.visitlog 
WHERE create_time >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND create_time < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY customer_id, apk_version, os_type, DATE(create_time);


-- fact_usage
REPLACE INTO focus_dw.fact_usage (customer_id, apk_version, os_type, visit_date, start_times, user_count) 
SELECT customer_id, apk_version, os_type, active_date, start_times, COUNT(1) 
FROM focus_ods.full_active_user 
WHERE active_date >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND active_date < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
GROUP BY customer_id, apk_version, os_type, active_date, start_times;
