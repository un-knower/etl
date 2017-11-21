#!/bin/bash
#
# 日志解析
# adv_visitlog -> fact_active
# 可选任务周期：天


source $ETL_HOME/common/db_util.sh


function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    # 第一次取全量数据，之后取前一天数据
    if [[ $is_first -ne 1 ]]; then
        time_filter="AND create_time >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY"
        debug "Got time filter: $time_filter"

        # 任务重做
        time_filter2="AND active_date = $prev_day"
        debug "Got time filter: $time_filter2"
    fi

    # 设置数据库连接
    debug "Set database connection: ${AD_ODS[@]}"
    set_db ${AD_ODS[@]}

    # 获取增量访问日志
    log_task $LOG_LEVEL_INFO "Get incremental access log from table: adv_visitlog"
    echo "SELECT
      androidid,
      city_id,
      IF(model > '', model, NULL),
      CASE nettype WHEN 1 THEN 2 WHEN 2 THEN 1 else nettype END,
      IF(src > '', src, NULL),
      IF(sysver > '', sysver, NULL),
      IF(version > '', version, NULL),
      root,
      create_time
    FROM adv_visitlog
    WHERE androidid > '' AND custom_id > ''
    AND create_time < STR_TO_DATE('$run_time','%Y%m%d%H%i%s') $time_filter;
    " | exec_sql > $data_path/active.tmp

    # 解析访问日志得到日活跃
    log_task $LOG_LEVEL_INFO "Parse access log to get daily active"
    sort -t $'\t' -k 1 -k 9 $data_path/active.tmp | awk -F '\t' 'BEGIN{OFS=FS}{
        if($1 == device_id && substr($9,1,10) == create_date){
            if($2 > 0) city_id=$2
            if($3 != "NULL") model=$3
            if($4 > nettype) nettype=$4
            if($5 != "NULL") src=$5
            if($6 != "NULL") sysver=$6
            if($7 != "NULL") version=$7
            if($8 > 0) is_root=$8
        }else{
            if(device_id != ""){
                gsub("-","",create_date)
                print device_id,create_date,city_id,model,nettype,src,sysver,version,is_root
            }
            device_id=$1
            city_id=$2
            model=$3
            nettype=$4
            src=$5
            sysver=$6
            version=$7
            is_root=$8
            create_date=substr($9,1,10)
        }
    }END{
        gsub("-","",create_date)
        print device_id,create_date,city_id,model,nettype,src,sysver,version,is_root,create_date
    }' | sort > $data_path/fact_active.tmp

    # 设置数据库连接
    debug "Set database connection: ${AD_DW[@]}"
    set_db ${AD_DW[@]}

    # 获取客户端其他信息（创建日期）
    log_task $LOG_LEVEL_INFO "Get device other information from table: dim_device"
    echo "SELECT device_id, DATE_FORMAT(create_time, '%Y%m%d') FROM dim_device;" | exec_sql | sort > $data_path/dim_device.tmp
    join -t "$sep" $data_path/fact_active.tmp $data_path/dim_device.tmp | mysql_data_conv > $data_path/fact_active.txt

    # 任务重做
    if [[ $redo_flag -eq 1 ]]; then
        log_task $LOG_LEVEL_INFO "Redo task, delete history data"
        echo "DELETE FROM fact_active WHERE 1 = 1 $time_filter2;" | exec_sql
    fi

    # 导入数据
    log_task $LOG_LEVEL_INFO "Load data to table: fact_active"
    echo "LOAD DATA LOCAL INFILE '$data_path/fact_active.txt' INTO TABLE fact_active
    (device_id,active_date,city_id,model,nettype,src,sysver,version,is_root,create_date)
    SET date_diff=DATEDIFF(active_date, create_date);
    " | exec_sql
}
execute "$@"