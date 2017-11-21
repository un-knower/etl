SELECT
	a.customer_id,
	COUNT(DISTINCT a.uuid, IF(a.date_diff = 0, 1, NULL)),
	COUNT(DISTINCT a.uuid, IF(a.date_diff = 1, 1, NULL)) / COUNT(DISTINCT a.uuid, IF(a.date_diff = 0 AND a.create_date < CURDATE() - INTERVAL 1 DAY, 1, NULL)) * 100,
	COUNT(DISTINCT a.uuid, IF(a.date_diff = 3, 1, NULL)) / COUNT(DISTINCT a.uuid, IF(a.date_diff = 0 AND a.create_date < CURDATE() - INTERVAL 3 DAY, 1, NULL)) * 100,
	COUNT(DISTINCT a.uuid, IF(a.date_diff = 6, 1, NULL)) / COUNT(DISTINCT a.uuid, IF(a.date_diff = 0 AND a.create_date < CURDATE() - INTERVAL 6 DAY, 1, NULL)) * 100
FROM fact_active a
INNER JOIN dim_device b
ON a.uuid = b.uuid
AND b.platform = 'android'
AND a.create_date >= CURDATE() - INTERVAL 7 DAY
GROUP BY a.customer_id;