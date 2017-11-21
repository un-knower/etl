#!/bin/bash
#
# 全量用户


source $ETL_HOME/common/db_util.sh


function execute()
{
    # 设置数据库连接
    set_db ${FOCUS_DW[@]}

    if [[ -z "$is_first" || $is_first -eq 0 ]]; then
        time_filter="AND visit_time >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY"
    fi

    log_task $LOG_LEVEL_INFO "Export data to local file: $data_path/fact_device.tmp"
    echo "SELECT uuid, username, customer_id, init_app_version, app_version, os_type, activate_date, update_date FROM 
    ( SELECT uuid, username, customer_id, init_app_version, app_version, os_type, activate_date, update_date 
      FROM fact_device 
    UNION ALL 
      SELECT uuid, username, customer_id, app_version init_app_version, app_version, os_type, DATE(visit_time) visit_date, DATE(visit_time) update_date 
      FROM focus_ods.visitlog 
      WHERE visit_time < STR_TO_DATE('$run_time','%Y%m%d%H%i%s') 
      $time_filter 
    ) t 
    ORDER BY uuid ASC, update_date ASC;
    " | exec_sql > $data_path/fact_device.tmp

    log_task $LOG_LEVEL_INFO "Merge data"
    awk -F '\t' 'BEGIN{OFS=FS}{
        if($1 == p_uuid){
            if($5 != p_version){
                app_version=$5
                update_date=$7
            }
        }else{
            if(p_uuid != "") print p_uuid,username,customer_id,init_app_version,app_version,os_type,activate_date,update_date
            username=$2
            customer_id=$3
            init_app_version=$4
            app_version=$5
            os_type=$6
            activate_date=$7
            update_date=$8
        }
        p_uuid=$1
        p_version=$5
    } END {
        print p_uuid,username,customer_id,init_app_version,app_version,os_type,activate_date,update_date
    }' $data_path/fact_device.tmp > $data_path/fact_device.txt

    # 导入表
    log_task $LOG_LEVEL_INFO "Import data file: $data_path/fact_device.txt to table: fact_device"
    echo "LOCK TABLES fact_device WRITE;
    TRUNCATE TABLE fact_device;
    LOAD DATA LOCAL INFILE '$data_path/fact_device.txt' INTO TABLE fact_device (uuid,username,customer_id,init_app_version,app_version,os_type,activate_date,update_date);
    UNLOCK TABLES;
    " | exec_sql
}
execute "$@"