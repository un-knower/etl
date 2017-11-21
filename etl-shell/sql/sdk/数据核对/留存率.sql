SET @customer_id = 'SJDK0087004';
SET @create_date = '2016-01-26';

-- 新增
SELECT COUNT(1)
FROM
	(
		SELECT uuid, DATE(MIN(createtime)) create_date
		FROM ods_device_visitlog
		WHERE customid = @customer_id
		GROUP BY uuid
		HAVING create_date = @create_date
	) t;

-- 次日留存
SELECT COUNT(1)
FROM
	(
		SELECT uuid, DATE(MIN(createtime)) create_date
		FROM ods_device_visitlog
		WHERE customid = @customer_id
		GROUP BY uuid
		HAVING create_date = @create_date
	) a
INNER JOIN (
	SELECT uuid, DATE(createtime) active_date
	FROM ods_device_visitlog
	WHERE customid = @customer_id
	AND createtime >= @create_date + INTERVAL 1 DAY
	AND createtime < @create_date + INTERVAL 2 DAY
	GROUP BY uuid
) b ON a.uuid = b.uuid;