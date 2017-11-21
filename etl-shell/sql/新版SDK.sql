-- 留存率统计
SELECT 
	b.platform,
	STR_TO_DATE(a.create_date, '%Y%m%d'),
	COUNT(DISTINCT a.uuid, IF(a.date_diff = 0, 1, NULL)) new_cnt,
	ROUND(COUNT(DISTINCT a.uuid, IF(a.date_diff = 1, 1, NULL)) / COUNT(DISTINCT a.uuid, IF(a.date_diff = 0, 1, NULL)) * 100, 2) keep_pct,
	ROUND(COUNT(DISTINCT a.uuid, IF(a.date_diff >= 1, 1, NULL)) / COUNT(DISTINCT a.uuid, IF(a.date_diff = 0, 1, NULL)) * 100, 2) keep_pct_1,
	ROUND(COUNT(DISTINCT a.uuid, IF(a.date_diff >= 2, 1, NULL)) / COUNT(DISTINCT a.uuid, IF(a.date_diff = 0, 1, NULL)) * 100, 2) keep_pct_2,
	ROUND(COUNT(DISTINCT a.uuid, IF(a.date_diff >= 3, 1, NULL)) / COUNT(DISTINCT a.uuid, IF(a.date_diff = 0, 1, NULL)) * 100, 2) keep_pct_3,
	ROUND(COUNT(DISTINCT a.uuid, IF(a.date_diff >= 4, 1, NULL)) / COUNT(DISTINCT a.uuid, IF(a.date_diff = 0, 1, NULL)) * 100, 2) keep_pct_4,
	ROUND(COUNT(DISTINCT a.uuid, IF(a.date_diff >= 5, 1, NULL)) / COUNT(DISTINCT a.uuid, IF(a.date_diff = 0, 1, NULL)) * 100, 2) keep_pct_5,
	ROUND(COUNT(DISTINCT a.uuid, IF(a.date_diff >= 6, 1, NULL)) / COUNT(DISTINCT a.uuid, IF(a.date_diff = 0, 1, NULL)) * 100, 2) keep_pct_6,
	ROUND(COUNT(DISTINCT a.uuid, IF(a.date_diff >= 7, 1, NULL)) / COUNT(DISTINCT a.uuid, IF(a.date_diff = 0, 1, NULL)) * 100, 2) keep_pct_7,
	ROUND(COUNT(DISTINCT a.uuid, IF(a.date_diff >= 14, 1, NULL)) / COUNT(DISTINCT a.uuid, IF(a.date_diff = 0, 1, NULL)) * 100, 2) keep_pct_14,
	ROUND(COUNT(DISTINCT a.uuid, IF(a.date_diff >= 30, 1, NULL)) / COUNT(DISTINCT a.uuid, IF(a.date_diff = 0, 1, NULL)) * 100, 2) keep_pct_30 
FROM fact_active a INNER JOIN dim_device b ON a.uuid = b.uuid AND a.log_type = 1
GROUP BY 1,2;