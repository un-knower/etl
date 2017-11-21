#!/bin/bash
#
# 开放数据
# 可用任务周期：天


source $ETL_HOME/common/db_util.sh


function execute()
{
    # 设置数据库连接
    debug "Set database connection: ${BASE_DW[@]}"
    set_db ${BASE_DW[@]}

    # 旧版SDK
    log_task $LOG_LEVEL_INFO  "Get old sdk data"
    echo "SELECT a.install_date, a.customer_id, a.init_install_cnt, a.install_cnt, IFNULL(b.init_activate_count, 0) init_activate_cnt, IFNULL(b.activate_count, 0) activate_cnt FROM 
    ( SELECT install_date, customer_id, COUNT(1) init_install_cnt, COUNT(IF(aid_status = 0 AND imsi_status = 0, 1, NULL)) install_cnt 
      FROM base_dw.fact_device 
      WHERE customer_id > '' 
      AND install_date = STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY 
      GROUP BY install_date, customer_id 
    ) a 
    LEFT JOIN 
    ( SELECT activate_date, customer_id, COUNT(1) init_activate_count, COUNT(IF(aid_status = 0 AND imsi_status = 0, 1, NULL)) activate_count 
      FROM base_dw.fact_device 
      WHERE customer_id > '' 
      AND is_activated = 1 
      AND activate_date = STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY 
      GROUP BY activate_date, customer_id 
    ) b 
    ON a.install_date = b.activate_date 
    AND a.customer_id = b.customer_id 
    UNION ALL 
    SELECT b.activate_date, b.customer_id, IFNULL(a.init_install_count, 0) init_install_cnt, IFNULL(a.install_count, 0) install_cnt, b.init_activate_cnt, b.activate_cnt FROM 
    ( SELECT install_date, customer_id, COUNT(1) init_install_count, COUNT(IF(aid_status = 0 AND imsi_status = 0, 1, NULL)) install_count 
      FROM base_dw.fact_device 
      WHERE customer_id > '' 
      AND install_date = STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY 
      GROUP BY install_date, customer_id 
    ) a 
    RIGHT JOIN 
    ( SELECT activate_date, customer_id, COUNT(1) init_activate_cnt, COUNT(IF(aid_status = 0 AND imsi_status = 0, 1, NULL)) activate_cnt 
      FROM base_dw.fact_device 
      WHERE customer_id > '' 
      AND is_activated = 1 
      AND activate_date = STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY 
      GROUP BY activate_date, customer_id 
    ) b 
    ON a.install_date = b.activate_date 
    AND a.customer_id = b.customer_id 
    WHERE a.init_install_count = 0 
    AND init_activate_cnt > 0;
    " | exec_sql > $data_path/old_sdk.tmp

    # 设置数据库连接
    debug "Set database connection: ${SDK_DW[@]}"
    set_db ${SDK_DW[@]}

    # 新版SDK
    log_task $LOG_LEVEL_INFO  "Get new sdk data"
    echo "SELECT DATE_FORMAT(a.create_time, '%Y-%m-%d'), a.customer_id, 0, 0, 0, COUNT(1)
    FROM fact_client a
    INNER JOIN dim_device b
    ON a.uuid = b.uuid
    AND b.is_exists = 0
    AND a.app_key = 'jz-yaya'
    AND a.create_date = STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY
    GROUP BY a.customer_id;
    " | exec_sql > $data_path/new_sdk.tmp

    # 合并数据
    log_task $LOG_LEVEL_INFO  "Merge old sdk data with new sdk data"
    cat $data_path/old_sdk.tmp $data_path/new_sdk.tmp | sort | awk -F '\t' 'BEGIN{OFS=FS}{
        if($2 == customer_id){
            init_install_cnt=init_install_cnt + $3
            install_cnt=install_cnt + $4
            init_activate_cnt=init_activate_cnt + $5
            activate_cnt=activate_cnt + $6
        }else{
            if(customer_id != "") print create_date,customer_id,init_install_cnt,install_cnt,init_activate_cnt,activate_cnt
            create_date=$1
            customer_id=$2
            init_install_cnt=$3
            install_cnt=$4
            init_activate_cnt=$5
            activate_cnt=$6
        }
    } END {
        print create_date,customer_id,init_install_cnt,install_cnt,init_activate_cnt,activate_cnt
    }' > $data_path/open_data.txt

    # 设置数据库连接
    debug "Set database connection: ${CUSDATA[@]}"
    set_db ${CUSDATA[@]}

    # 任务重做
    if [[ $redo_flag -eq 1 ]]; then
        log_task $LOG_LEVEL_INFO "Redo task, delete history data"
        echo "DELETE FROM t_channeldaily WHERE cdate = '$prev_day1';" | exec_sql
    fi

    # 导入数据
    log_task $LOG_LEVEL_INFO "Load data to table: t_channeldaily"
    echo "LOAD DATA LOCAL INFILE '$data_path/open_data.txt' IGNORE INTO TABLE t_channeldaily
    (cdate,chacode,inssum,fsum,actsum,csum);
    " | exec_sql
}
execute "$@"