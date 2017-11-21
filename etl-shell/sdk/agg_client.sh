#!/bin/bash
#
# 聚合fact_client


source $ETL_HOME/common/db_util.sh


function execute()
{
    echo "CREATE TABLE agg_lc_01_client (
      app_key VARCHAR(50),
      the_year SMALLINT(6),
      half_year TINYINT(4),
      quarter_num TINYINT(4),
      month_num TINYINT(4),
      fact_count INT(11),
      PRIMARY KEY (app_key, the_year, half_year, quarter_num, month_num)
    ) ENGINE=MyISAM;
    INSERT INTO agg_lc_01_client SELECT
      a.app_key,
      b.the_year,
      b.half_year,
      b.quarter_num,
      b.month_num,
      COUNT(1)
    FROM fact_client a
    INNER JOIN dim_date b
    ON a.create_date = b.id
    GROUP BY a.app_key, b.the_year, b.half_year, b.quarter_num, b.month_num;

    CREATE TABLE agg_l_02_client (
      app_key VARCHAR(50),
      create_date INT(11),
      fact_count INT(11),
      PRIMARY KEY (app_key, create_date)
    ) ENGINE=MyISAM;
    INSERT INTO agg_l_02_client SELECT app_key, create_date, COUNT(1) FROM fact_client GROUP BY app_key, create_date;

    CREATE TABLE agg_l_03_client (
      app_key VARCHAR(50),
      create_date INT(11),
      customer_id VARCHAR(50) COMMENT '渠道号',
      fact_count INT(11),
      PRIMARY KEY (app_key, create_date, customer_id)
    ) ENGINE=MyISAM;
    INSERT INTO agg_l_03_client SELECT app_key, create_date, customer_id, COUNT(1) FROM fact_client GROUP BY app_key, create_date, customer_id;

    CREATE TABLE agg_l_04_client (
      app_key VARCHAR(50),
      create_date INT(11),
      pkg_path TINYINT(4) NOT NULL DEFAULT '0' COMMENT '安装目录 1:system目录 2:data目录 3:sd卡目录 4:vendor目录',
      fact_count INT(11),
      PRIMARY KEY (app_key, create_date, pkg_path)
    ) ENGINE=MyISAM;
    INSERT INTO agg_l_04_client SELECT app_key, create_date, pkg_path, COUNT(1) FROM fact_client GROUP BY app_key, create_date, pkg_path;

    CREATE TABLE agg_l_05_client (
      app_key VARCHAR(50),
      create_date INT(11),
      version VARCHAR(10) COMMENT '当前版本',
      fact_count INT(11),
      PRIMARY KEY (app_key, create_date, version)
    ) ENGINE=MyISAM;
    INSERT INTO agg_l_05_client SELECT app_key, create_date, version, COUNT(1) FROM fact_client GROUP BY app_key, create_date, version;

    CREATE TABLE agg_l_06_client (
      app_key VARCHAR(50),
      create_date INT(11),
      is_upgrade TINYINT(4) NOT NULL DEFAULT '0' COMMENT '是否升级 0:否 1:是',
      fact_count INT(11),
      PRIMARY KEY (app_key, create_date, is_upgrade)
    ) ENGINE=MyISAM;
    INSERT INTO agg_l_06_client SELECT app_key, create_date, is_upgrade, COUNT(1) FROM fact_client GROUP BY app_key, create_date, is_upgrade;

    CREATE TABLE agg_l_07_client (
      app_key VARCHAR(50),
      create_date INT(11),
      version VARCHAR(10) COMMENT '当前版本',
      is_upgrade TINYINT(4) NOT NULL DEFAULT '0' COMMENT '是否升级 0:否 1:是',
      fact_count INT(11),
      PRIMARY KEY (app_key, create_date, version, is_upgrade)
    ) ENGINE=MyISAM;
    INSERT INTO agg_l_07_client SELECT app_key, create_date, version, is_upgrade, COUNT(1) FROM fact_client GROUP BY app_key, create_date, version, is_upgrade;
    " | exec_sql
}
execute "$@"