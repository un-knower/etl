-- 1、使用次数
CREATE TABLE IF NOT EXISTS fact_open (
  uuid VARCHAR(64),
  app_key VARCHAR(50),
  clnt VARCHAR(32),
  version VARCHAR(50),
  stat_date INT,
  open_times INT COMMENT '打开次数',
  PRIMARY KEY(uuid, app_key, stat_date)
) ENGINE=MyISAM COMMENT='打开次数';
CREATE INDEX idx_uuid ON fact_open (uuid);
CREATE INDEX idx_app_key ON fact_open (app_key);
CREATE INDEX idx_clnt ON fact_open (clnt);
CREATE INDEX idx_version ON fact_open (version);
CREATE INDEX idx_stat_date ON fact_open (stat_date);
CREATE INDEX idx_open_times ON fact_open (open_times);

INSERT INTO fact_open (uuid, app_key, clnt, version, stat_date, open_times) 
SELECT uuid, productkey, clnt, version, DATE_FORMAT(insertdate, '%Y%m%d'), COUNT(1) FROM bw_eventdata 
WHERE event = 'f_open' 
AND insertdate >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND insertdate < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
AND uuid > '' AND productkey > '' AND clnt > '' AND version > '' 
GROUP BY uuid, productkey, DATE_FORMAT(insertdate, '%Y%m%d');


-- 2、使用时长
CREATE TABLE IF NOT EXISTS fact_duration (
  id BIGINT,
  uuid VARCHAR(64),
  app_key VARCHAR(50),
  clnt VARCHAR(32),
  version VARCHAR(50),
  stat_date INT,
  duration INT COMMENT '时长',
  PRIMARY KEY(id)
) ENGINE=MyISAM COMMENT='使用时长';
CREATE INDEX idx_uuid ON fact_duration (uuid);
CREATE INDEX idx_app_key ON fact_duration (app_key);
CREATE INDEX idx_clnt ON fact_duration (clnt);
CREATE INDEX idx_version ON fact_duration (version);
CREATE INDEX idx_stat_date ON fact_duration (stat_date);
CREATE INDEX idx_duration ON fact_duration (duration);

INSERT INTO fact_duration (id, uuid, app_key, clnt, version, stat_date, duration) 
SELECT id, uuid, productkey, clnt, version, DATE_FORMAT(insertdate, '%Y%m%d'), CEIL(acc / 1000) FROM bw_eventdata 
WHERE event = 'f_exit' 
AND insertdate >= STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') - INTERVAL 1 DAY 
AND insertdate < STR_TO_DATE('#run_time#','%Y%m%d%H%i%s') 
AND uuid > '' AND productkey > '' AND clnt > '' AND version > '' AND acc > 0 AND acc < 3600000;
