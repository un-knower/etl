#!/bin/bash
#
# 解析sdk_ods.ums_clientusinglog获取用户session信息
# sdk_dw.fact_session
# 可用任务周期：时/天/周/月


source $ETL_HOME/common/db_util.sh


function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    # 第一次取全量数据，之后取前一天数据
    if [[ $is_first -ne 1 ]]; then
        time_filter="AND insertdate >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY"
        debug "Got time filter: $time_filter"

        # 任务重做
        time_filter1="AND create_time >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY AND create_time < STR_TO_DATE('$run_time', '%Y%m%d%H%i%s')"
        debug "Got time filter: $time_filter1"
    fi

    # 设置数据库连接
    debug "Set database connection: ${SDK_DW[@]}"
    set_db ${SDK_DW[@]}

    # 任务重做
    if [[ $redo_flag -eq 1 ]]; then
        log_task $LOG_LEVEL_INFO "Redo task, delete history data"
        echo "DELETE FROM fact_session WHERE 1 = 1 $time_filter1;" | exec_sql
    fi

    # 获取最新用户App使用日志
    log_task $LOG_LEVEL_INFO "Get the latest user app using log from table: ums_clientusinglog"
    echo "SELECT
      deviceid,
      appkey,
      session_id,
      MIN(start_millis),
      version,
      ROUND(SUM(duration) / 1000)
    FROM sdk_ods.ums_clientusinglog
    WHERE deviceid > '' AND appkey > '' AND session_id > ''
    AND insertdate < STR_TO_DATE('$run_time','%Y%m%d%H%i%s') $time_filter
    GROUP BY deviceid, appkey, session_id
    ORDER BY deviceid ASC, appkey ASC, start_millis ASC;
    " | exec_sql > $data_path/start.tmp

    # 获取上次启动时间
    log_task $LOG_LEVEL_INFO "Parse user app using log to get previous session"
    awk -F '\t' 'BEGIN{OFS=FS}{
        if($1 == uuid && $2 == app_key){
            print $1"$"$2,$0,create_time
        }else{
            if(is_first != 1){
                print $1"$"$2,$0 >> "'$data_path/first_start.tmp'"
            }else{
                print $1"$"$2,$0,"NULL"
            }
        }
        uuid=$1
        app_key=$2
        create_time=$4
    }' is_first=$is_first $data_path/start.tmp > $data_path/fact_session.tmp

    # 获取上次启动时间 非首次执行
    if [[ $is_first -ne 1 ]]; then
        # 获取上一次启动信息
        log_task $LOG_LEVEL_INFO "Get previous session from table: fact_session"
        echo "SELECT
          CONCAT(uuid, '$', app_key),
          session_id,
          MAX(create_time),
          duration
        FROM
        (
          SELECT
            uuid,
            app_key,
            session_id,
            create_time,
            duration
          FROM fact_session
          ORDER BY create_time DESC
        ) t
        GROUP BY CONCAT(uuid, '$', app_key);
        " > $data_path/prev_start.tmp

        # 排序、关联、合并跨天session
        debug "Sort data: $data_path/first_start.tmp"
        sort $data_path/first_start.tmp -o $data_path/first_start.tmp
        debug "Sort data: $data_path/prev_start.tmp"
        sort $data_path/prev_start.tmp -o $data_path/prev_start.tmp
        debug "Join data: $data_path/first_start.tmp $data_path/prev_start.tmp"
        join -t "$sep" -a 1 $data_path/first_start.tmp $data_path/prev_start.tmp | awk -F '\t' 'BEGIN{OFS=FS}{
            if($4 == $8){
                print $1,$2,$3,$4,$9,$6,$7 + $10,"NULL"
            }else{
                print $1,$2,$3,$4,$5,$6,$7,$9
            }
        }' >> $data_path/fact_session.tmp
    fi

    # 获取客户端其他信息（安装渠道号）
    log_task $LOG_LEVEL_INFO "Get client other information from table: fact_client"
    echo "SELECT CONCAT(uuid, '$', app_key), customer_id FROM fact_client;" | exec_sql > $data_path/fact_client.tmp
    # 排序、关联
    debug "Sort data: $data_path/fact_session.tmp"
    sort $data_path/fact_session.tmp -o $data_path/fact_session.tmp
    debug "Sort data: $data_path/fact_client.tmp"
    sort $data_path/fact_client.tmp -o $data_path/fact_client.tmp
    join -t "$sep" -o 1.2 1.3 1.4 1.5 1.6 1.7 2.2 1.8 $data_path/fact_session.tmp $data_path/fact_client.tmp | mysql_data_conv > $data_path/fact_session.txt

    # 导入数据
    log_task $LOG_LEVEL_INFO "Load data to table: fact_session"
    echo "LOAD DATA LOCAL INFILE '$data_path/fact_session.txt' REPLACE INTO TABLE fact_session
    (uuid,app_key,session_id,create_time,version,@vduration,customer_id,prev_time,create_date,date_diff)
    SET create_date=DATE_FORMAT(create_time, '%Y%m%d'), duration=IF(@vduration > 0, @vduration, IF(@vduration = 0, 1, ABS(@vduration))), date_diff=IFNULL(DATEDIFF(create_time, prev_time), -1);
    " | exec_sql
}
execute "$@"