-- 补数据（基线visitlog数据比device少）
INSERT IGNORE INTO base_dw.fact_active (uuid, customer_id, app_version, visit_date, init_app_version, install_date, day_diff, visit_times) 
SELECT uuid, customer_id, init_app_version, install_date, init_app_version, install_date, 0, 1 FROM base_dw.fact_device;

-- 基线服务-渠道整体留存率
-- new_cnt 新增用户
-- keep_pct_1 次日留存率
-- keep_pct_2 2天后留存率

SET @begin_date = '2015-10-01';
SET @the_day = '2015-10-30';

SELECT 
  customer_id,
  COUNT(
    DISTINCT uuid,
    IF(day_diff = 0 AND install_date < @the_day, 1, NULL)
  ) new_cnt,
  COUNT(
    DISTINCT uuid,
    IF(day_diff = 1 AND visit_date < @the_day, 1, NULL)
  ) / 
  COUNT(
    DISTINCT uuid,
    IF(day_diff = 0 AND install_date < @the_day - INTERVAL 1 DAY, 1, NULL)
  ) keep_pct_1,
  COUNT(
    DISTINCT uuid,
    IF(day_diff >= 2 AND visit_date < @the_day - INTERVAL 1 DAY, 1, NULL)
  ) / 
  COUNT(
    DISTINCT uuid,
    IF(day_diff = 0 AND install_date < @the_day - INTERVAL 2 DAY, 1, NULL)
  ) keep_pct_2,
  COUNT(
    DISTINCT uuid,
    IF(day_diff >= 3 AND visit_date < @the_day - INTERVAL 2 DAY, 1, NULL)
  ) / 
  COUNT(
    DISTINCT uuid,
    IF(day_diff = 0 AND install_date < @the_day - INTERVAL 3 DAY, 1, NULL)
  ) keep_pct_3,
  COUNT(
    DISTINCT uuid,
    IF(day_diff >= 4 AND visit_date < @the_day - INTERVAL 3 DAY, 1, NULL)
  ) / 
  COUNT(
    DISTINCT uuid,
    IF(day_diff = 0 AND install_date < @the_day - INTERVAL 4 DAY, 1, NULL)
  ) keep_pct_4,
  COUNT(
    DISTINCT uuid,
    IF(day_diff >= 5 AND visit_date < @the_day - INTERVAL 4 DAY, 1, NULL)
  ) / 
  COUNT(
    DISTINCT uuid,
    IF(day_diff = 0 AND install_date < @the_day - INTERVAL 5 DAY, 1, NULL)
  ) keep_pct_5,
  COUNT(
    DISTINCT uuid,
    IF(day_diff >= 6 AND visit_date < @the_day - INTERVAL 5 DAY, 1, NULL)
  ) / 
  COUNT(
    DISTINCT uuid,
    IF(day_diff = 0 AND install_date < @the_day - INTERVAL 6 DAY, 1, NULL)
  ) keep_pct_6,
  COUNT(
    DISTINCT uuid,
    IF(day_diff >= 7 AND visit_date < @the_day - INTERVAL 6 DAY, 1, NULL)
  ) / 
  COUNT(
    DISTINCT uuid,
    IF(day_diff = 0 AND install_date < @the_day - INTERVAL 7 DAY, 1, NULL)
  ) keep_pct_7,
  COUNT(
    DISTINCT uuid,
    IF(day_diff >= 14 AND visit_date < @the_day - INTERVAL 13 DAY, 1, NULL)
  ) / 
  COUNT(
    DISTINCT uuid,
    IF(day_diff = 0 AND install_date < @the_day - INTERVAL 14 DAY, 1, NULL)
  ) keep_pct_14,
  COUNT(
    DISTINCT uuid,
    IF(day_diff >= 30 AND visit_date < @the_day - INTERVAL 29 DAY, 1, NULL)
  ) / 
  COUNT(
    DISTINCT uuid,
    IF(day_diff = 0 AND install_date < @the_day - INTERVAL 30 DAY, 1, NULL)
  ) keep_pct_30 
FROM fact_active 
WHERE install_date >= @begin_date
GROUP BY customer_id;