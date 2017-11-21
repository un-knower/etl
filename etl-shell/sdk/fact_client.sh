#!/bin/bash
#
# 解析sdk_ods.ods_device_visitlog获取客户端信息
# sdk_dw.fact_client
# 可用任务周期：时/天/周/月


source $ETL_HOME/common/db_util.sh


function execute()
{
    # 第一次取全量数据，之后取上一个周期数据
    if [[ $is_first -ne 1 ]]; then
        time_filter="AND createtime >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY"
        debug "Got time filter: $time_filter"

        # 任务重做
        time_filter1="AND update_time >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY AND update_time < STR_TO_DATE('$run_time', '%Y%m%d%H%i%s')"
        debug "Got time filter: $time_filter1"
    fi

    debug "Set database connection: ${SDK_DW[@]}"
    set_db ${SDK_DW[@]}

    # 任务重做
    if [[ $redo_flag -eq 1 ]]; then
        log_task $LOG_LEVEL_INFO "Redo task, delete history data"
        echo "DELETE FROM fact_client WHERE 1 = 1 $time_filter1;" | exec_sql
    fi

    # 获取最新用户访问日志
    log_task $LOG_LEVEL_INFO "Get the latest user access log from table: ods_device_visitlog"
    echo "SELECT
      uuid,
      app_key,
      customer_id,
      version,
      pkg_path,
      create_time,
      update_time,
      init_version,
      create_date,
      version_no
    FROM
    (
      SELECT
        uuid,
        app_key,
        customer_id,
        version,
        pkg_path,
        create_time,
        update_time,
        init_version,
        create_date,
        0 version_no
      FROM fact_client
      UNION ALL
      SELECT
        uuid,
        appkey,
        customid,
        version,
        pkgpath,
        createtime,
        createtime,
        version,
        DATE_FORMAT(createtime, '%Y%m%d'),
        versioncode
      FROM sdk_ods.ods_device_visitlog
      WHERE uuid > '' AND appkey > '' AND customid > '' AND version > '' AND createtime > '2013-01-14'
      AND createtime < STR_TO_DATE('$run_time','%Y%m%d%H%i%s') $time_filter
    ) t
    ORDER BY uuid ASC, app_key ASC, update_time ASC;
    " | exec_sql > $data_path/client.tmp

    # 解析用户访问日志得到全量客户端信息
    log_task $LOG_LEVEL_INFO "Parse user access log to get full client information"
    awk -F '\t' 'BEGIN{OFS=FS}{
        if($1 == uuid && $2 == app_key){
            version=$4
            update_time=$7
        }else{
            if(uuid != ""){
                # 沉默用户判断
                stime=systime()
                ctime=mktime(substr(create_time,1,4)" "substr(create_time,6,2)" "substr(create_time,9,2)" 00 00 00")
                utime=mktime(substr(update_time,1,4)" "substr(update_time,6,2)" "substr(update_time,9,2)" 00 00 00")
                # 创建日期与当前日期间隔天数
                time_diff1=int((stime - ctime) / 86400)
                # 创建日期与更新日期间隔天数
                time_diff2=int((utime - ctime) / 86400)
                if(time_diff1 >= 3 && time_diff2 <= 1){
                    is_silent=1
                }else{
                    is_silent=0
                }
                print uuid,app_key,customer_id,version,pkg_path,create_time,update_time,init_version,create_date,is_silent
            }
            uuid=$1
            app_key=$2
            customer_id=$3
            version=$4
            pkg_path=$5
            create_time=$6
            update_time=$7
            init_version=$8
            create_date=$9
        }
        # App版本
        print $4,$10 >> "'$data_path/version.txt'"
    } END {
        # 沉默用户判断
        stime=systime()
        ctime=mktime(substr(create_time,1,4)" "substr(create_time,6,2)" "substr(create_time,9,2)" 00 00 00")
        utime=mktime(substr(update_time,1,4)" "substr(update_time,6,2)" "substr(update_time,9,2)" 00 00 00")
        # 创建日期与当前日期间隔天数
        time_diff1=int((stime - ctime) / 86400)
        # 创建日期与更新日期间隔天数
        time_diff2=int((utime - ctime) / 86400)
        if(time_diff1 >= 3 && time_diff2 <= 1){
            is_silent=1
        }else{
            is_silent=0
        }
        print uuid,app_key,customer_id,version,pkg_path,create_time,update_time,init_version,create_date,is_silent
    }' $data_path/client.tmp | mysql_data_conv > $data_path/fact_client.txt

    # 去重
    debug "Remove duplicate data"
    sort -u $data_path/version.txt -o $data_path/version.txt
    # 导入App版本数据
    log_task $LOG_LEVEL_INFO "Load data to table: dim_version"
    echo "LOAD DATA LOCAL INFILE '$data_path/version.txt' IGNORE INTO TABLE dim_version (id, version_no);" | exec_sql

    # 导入数据
    log_task $LOG_LEVEL_INFO "Load data to table: fact_client"
    echo "LOCK TABLES fact_client WRITE;
    TRUNCATE TABLE fact_client;
    LOAD DATA LOCAL INFILE '$data_path/fact_client.txt' INTO TABLE fact_client
    (uuid,app_key,customer_id,version,pkg_path,create_time,update_time,init_version,create_date,is_silent);
    UNLOCK TABLES;
    " | exec_sql
}
execute "$@"