#!/bin/bash
#
# 基线服务-设备事实表
# base_dw.fact_device


source $ETL_HOME/common/db_util.sh


# 有效打分最少规则个数
MIN_RULE_COUNT=7


# 更新数据
function update_data()
{
    # 全量/首次/增量
    if [[ $do_full -eq 1 ]]; then
        debug "Do full data"
        time_filter="AND createtime < CURDATE() AND updatetime < CURDATE()"
    elif [[ $is_first -eq 1 ]]; then
        debug "Do for the first time"
        time_filter="AND ( createtime < STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') OR updatetime < STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') )"
    else
        debug "Do incremental data"
        time_filter="AND ( ( createtime >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY AND createtime < STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') ) OR ( updatetime >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY AND updatetime < STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') ) )"
    fi

    echo "SELECT a.uid, a.isrepeat, a.createtime, a.updatetime, IFNULL(b.customer_id, a.clt), a.vi, a.app_ver, a.originalstatus, a.changedstatus, a.aid, a.ms, a.install_date, a.isp_code, a.is_upgraded,
    b.aid_status, b.imsi_status, b.is_activated, b.activate_date FROM 
    (SELECT 
      uid,
      isrepeat,
      createtime,
      updatetime,
      clt,
      vi,
      IF(updatevi IS NULL OR updatevi = 0, vi, updatevi) app_ver,
      originalstatus,
      changedstatus,
      aid,
      ms,
      DATE(createtime) install_date,
      IFNULL(opt, 0) isp_code,
      IF(updatevi <> vi AND updatevi > 0, 1, 0) is_upgraded 
    FROM base_ods.device 
    WHERE 1 = 1 
    $time_filter 
    ) a 
    LEFT JOIN 
    base_dw.fact_device b 
    ON a.uid = b.uuid;
    " | exec_sql > $data_path/fact_device.txt

    echo "LOAD DATA LOCAL INFILE '$data_path/fact_device.txt' REPLACE INTO TABLE base_dw.fact_device 
    (uuid,is_repeated,create_time,update_time,customer_id,init_app_version,app_version,original_status,changed_status,android_id,imsi,install_date,isp_code,is_upgraded,aid_status,imsi_status,is_activated,activate_date);
    " | exec_sql
}

# Android ID是否重复
function andrid_status()
{
    log_task $LOG_LEVEL_INFO "Find android id that has appeared more than one time"
    echo "SELECT android_id, MIN(create_time) FROM base_dw.fact_device WHERE android_id > '' GROUP BY android_id HAVING COUNT(1) > 1;" | exec_sql | sort > $data_path/android_id.tmp

    # 获取新增Android ID
    log_task $LOG_LEVEL_INFO "Get incremental android id"
    awk -F '\t' '{ print $10 }' $data_path/fact_device.txt | sort -u > $data_path/android_id.tmp.new
    join -t "$sep" $data_path/android_id.tmp $data_path/android_id.tmp.new > $data_path/android_id.txt

    # 更新重复的Android ID
    log_task $LOG_LEVEL_INFO "Update android id status"
    echo "CREATE TEMPORARY TABLE tmp_android_id (android_id VARCHAR(50), create_time DATETIME);
    LOAD DATA LOCAL INFILE '$data_path/android_id.txt' INTO TABLE tmp_android_id;
    UPDATE base_dw.fact_device a INNER JOIN tmp_android_id b ON a.android_id = b.android_id AND a.aid_status = 0 AND a.create_time > b.create_time SET a.aid_status = 1;
    " | exec_sql

    # 更新createtime相同的数据
    echo "UPDATE base_dw.fact_device a 
    INNER JOIN (
      SELECT MIN(uuid) uid, android_id 
      FROM base_dw.fact_device 
      WHERE aid_status = 0 
      AND android_id > '' 
      GROUP BY android_id 
      HAVING COUNT(1) > 1 
    ) b 
    ON a.android_id = b.android_id 
    AND a.aid_status = 0 
    SET a.aid_status = 1 
    WHERE a.uuid <> b.uid;
    " | exec_sql
}

# IMSI是否重复
function imsi_status()
{
    log_task $LOG_LEVEL_INFO "Find imsi that has appeared more than one time"
    echo "SELECT imsi, MIN(create_time) FROM base_dw.fact_device WHERE imsi > '' AND imsi NOT IN ('Unknown') GROUP BY imsi HAVING COUNT(1) > 1;" | exec_sql | sort > $data_path/imsi.tmp

    # 获取新增IMSI
    log_task $LOG_LEVEL_INFO "Get incremental imsi"
    awk -F '\t' '{ print $11 }' $data_path/fact_device.txt | sort -u > $data_path/imsi.tmp.new
    join -t "$sep" $data_path/imsi.tmp $data_path/imsi.tmp.new > $data_path/imsi.txt

    # 更新重复的IMSI
    log_task $LOG_LEVEL_INFO "Update imsi status"
    echo "CREATE TEMPORARY TABLE tmp_imsi (imsi VARCHAR(50), create_time DATETIME);
    LOAD DATA LOCAL INFILE '$data_path/imsi.txt' INTO TABLE tmp_imsi;
    UPDATE base_dw.fact_device a INNER JOIN tmp_imsi b ON a.imsi = b.imsi AND a.imsi_status = 0 AND a.create_time > b.create_time SET a.imsi_status = 1;
    " | exec_sql

    # 更新createtime相同的数据
    echo "UPDATE base_dw.fact_device a 
    INNER JOIN (
      SELECT MIN(uuid) uid, imsi 
      FROM base_dw.fact_device 
      WHERE imsi_status = 0 
      AND imsi > '' 
      AND imsi NOT IN ('Unknown') 
      GROUP BY imsi 
      HAVING COUNT(1) > 1 
    ) b 
    ON a.imsi = b.imsi 
    AND a.imsi_status = 0 
    SET a.imsi_status = 1 
    WHERE a.uuid <> b.uid;
    " | exec_sql
}

function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    set_db ${BASE_DW[@]}

    update_data

    andrid_status

    imsi_status
}
execute "$@"