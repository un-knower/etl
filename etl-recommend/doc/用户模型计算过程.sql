CREATE TABLE IF NOT EXISTS click_event (
  device_id VARCHAR(50) COMMENT '设备ID',
  event_name VARCHAR(50) COMMENT '事件名称',
  info_id VARCHAR(50) COMMENT '资讯ID',
  click_time DATETIME COMMENT '点击时间'
) COMMENT='点击事件';

CREATE TABLE IF NOT EXISTS d_user_click (
  click_date DATE COMMENT '点击日期',
  device_id VARCHAR(50) COMMENT '设备ID',
  info_id VARCHAR(50) COMMENT '资讯ID',
  event_name VARCHAR(50) COMMENT '事件名称',
  click_count INT COMMENT '点击次数'
) COMMENT='用户点击次数-日汇总';

CREATE TABLE IF NOT EXISTS d_info_click (
  click_date DATE COMMENT '点击日期',
  info_id VARCHAR(50) COMMENT '资讯ID',
  user_count INT COMMENT '点击人数'
) COMMENT='资讯点击人数-日汇总';

-- 原始数据
TRUNCATE TABLE click_event;
INSERT INTO click_event VALUES 
('user-1', 'view', 'info-1', '2016-10-18 08:23:01'),
('user-1', 'share', 'info-1', '2016-10-18 08:25:11'),
('user-1', 'comment', 'info-1', '2016-10-18 08:26:30'),
('user-1', 'view', 'info-2', '2016-10-18 08:30:01'),
('user-1', 'view', 'info-2', '2016-10-18 09:01:02'),
('user-1', 'comment', 'info-2', '2016-10-18 09:02:10'),
('user-1', 'view', 'info-3', '2016-10-18 09:05:21'),
('user-2', 'view', 'info-1', '2016-10-18 12:20:10'),
('user-2', 'view', 'info-2', '2016-10-18 12:22:17'),
('user-2', 'share', 'info-2', '2016-10-18 12:26:20'),
('user-3', 'view', 'info-1', '2016-10-18 18:01:11'),
('user-3', 'view', 'info-3', '2016-10-18 18:05:18'),
('user-3', 'comment', 'info-3', '2016-10-18 18:06:25'),
('user-3', 'share', 'info-3', '2016-10-18 18:08:03'),
('user-1', 'view', 'info-4', '2016-10-19 08:30:01'),
('user-1', 'share', 'info-4', '2016-10-19 08:32:05'),
('user-1', 'view', 'info-5', '2016-10-19 08:35:06'),
('user-1', 'view', 'info-6', '2016-10-19 08:39:05'),
('user-2', 'view', 'info-4', '2016-10-19 12:20:05'),
('user-2', 'comment', 'info-4', '2016-10-19 12:22:10'),
('user-2', 'view', 'info-5', '2016-10-19 12:25:15'),
('user-2', 'view', 'info-6', '2016-10-19 12:28:11'),
('user-2', 'comment', 'info-6', '2016-10-19 12:30:10'),
('user-3', 'view', 'info-4', '2016-10-19 18:01:02'),
('user-3', 'view', 'info-5', '2016-10-19 18:05:10'),
('user-3', 'comment', 'info-5', '2016-10-19 18:10:01'),
('user-3', 'view', 'info-6', '2016-10-19 18:12:01'),
('user-3', 'view', 'info-6', '2016-10-19 18:15:05');

-- 中间数据
TRUNCATE TABLE d_user_click;
TRUNCATE TABLE d_info_click;
INSERT INTO d_user_click SELECT DATE(click_time), device_id, info_id, event_name, COUNT(1) FROM click_event GROUP by 1, 2 ,3, 4;
INSERT INTO d_info_click SELECT DATE(click_time), info_id, COUNT(DISTINCT device_id) FROM click_event GROUP BY 1, 2;

-- 计算数据
SELECT a.device_id, a.event_name, a.info_id, DATEDIFF('2016-10-20', a.click_date), a.click_count, b.user_count 
FROM d_user_click a INNER JOIN d_info_click b 
ON a.click_date = b.click_date AND a.info_id = b.info_id;
