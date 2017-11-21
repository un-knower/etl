#!/bin/bash
#
# 解析sdk_ods.ods_device_visitlog获取用户活跃信息
# sdk_dw.fact_active
# 可用任务周期：天


source $ETL_HOME/common/db_util.sh


function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    # 第一次取全量数据，之后取前一天数据
    if [[ $is_first -ne 1 ]]; then
        time_filter="AND createtime >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY"
        debug "Got time filter: $time_filter"

        # 启动信息
        time_filter1="AND create_time >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY"
        debug "Got time filter: $time_filter1"

        # 任务重做
        time_filter2="AND active_date = $prev_day"
        debug "Got time filter: $time_filter2"
    fi

    # 设置数据库连接
    debug "Set database connection: ${SDK_DW[@]}"
    set_db ${SDK_DW[@]}

    # 获取最新用户访问日志
    log_task $LOG_LEVEL_INFO "Get the latest user access log from table: ods_device_visitlog"
    echo "SELECT
      uuid,
      appkey,
      DATE_FORMAT(createtime, '%Y%m%d'),
      HOUR(createtime),
      TIMESTAMPDIFF(SECOND, createtime, time),
      CASE
      WHEN boottime IS NULL OR boottime <= 0 THEN 0
      WHEN boottime / 60000 < 1 THEN 1
      WHEN boottime / 60000 > 120 THEN 121
      ELSE ROUND(boottime / 60000)
      END,
      version,
      extsizeleft,
      romsizeleft,
      CASE
      WHEN screenon IS NULL OR screenon <= 0 THEN 0
      WHEN screenon > 100 THEN 100
      ELSE screenon
      END,
      IF(battery > 0 AND battery <= 100, battery, 0),
      logtype,
      IF(city > '', city, 'unknown'),
      IF(region > '', region, 'unknown'),
      IF(country > '', country, 'unknown'),
      clientip
    FROM sdk_ods.ods_device_visitlog
    WHERE uuid > '' AND appkey > '' AND customid > '' AND version > '' AND logtype > 0 AND createtime > '2013-01-14'
    AND createtime < STR_TO_DATE('$run_time','%Y%m%d%H%i%s') $time_filter
    ORDER BY uuid ASC, appkey ASC, logtype ASC, createtime ASC;
    " | exec_sql > $data_path/active.tmp

    # 解析用户访问日志得到日活跃
    log_task $LOG_LEVEL_INFO "Parse user access log to get daily active"
    awk -F '\t' 'BEGIN{
        OFS=FS
        IGNORECASE=1
    }{
        if($1 == uuid && $2 == app_key && $3 == active_date && $12 == log_type){
            active_hours[$4]=$4
            time_diff=$5
            if($6 > boottime) boottime=$6
            sdcard_size=$8
            rom_size=$9
            if($10 > screen_on) screen_on=$10
            if($11 > 0) battery[$11]++
            if(city ~ /^unknown$/) city=$13
            if(region ~ /^unknown$/) region=$14
            if(country ~ /^unknown$/) country=$15
            client_ip=$16
            visit_times++
        }else{
            if(uuid != ""){
                len=asort(active_hours,sorted_hours);
                hours=sorted_hours[1]
                for(i=2;i<=len;i++){
                    hours=hours","sorted_hours[i]
                }

                # 日志类型，用户主动/定时联网
                if(log_type == 1){
                    print uuid"$"app_key"$"active_date,uuid,app_key,active_date,hours,time_diff,boottime,version,sdcard_size,rom_size,screen_on,length(battery),log_type,city,region,country,client_ip,visit_times >> "'$data_path'/fact_active-1.tmp"
                }else{
                    print uuid"$"app_key,uuid,app_key,active_date,hours,time_diff,boottime,version,sdcard_size,rom_size,screen_on,length(battery),log_type,city,region,country,client_ip,visit_times,0
                }
            }
            uuid=$1
            app_key=$2
            active_date=$3
            delete active_hours
            active_hours[$4]=$4
            time_diff=$5
            boottime=$6
            version=$7
            sdcard_size=$8
            rom_size=$9
            screen_on=$10
            delete battery
            if($11 > 0) battery[$11]++
            log_type=$12
            if($13 ~ /^unknown$/){
                city="unknown"
            }else{
                city=$13
            }
            if($14 ~ /^unknown$/){
                region="unknown"
            }else{
                region=$14
            }
            if($15 ~ /^unknown$/){
                country="unknown"
            }else{
                country=$15
            }
            client_ip=$16
            visit_times=1
        }
    } END {
        len=asort(active_hours,sorted_hours);
        hours=sorted_hours[1]
        for(i=2;i<=len;i++){
            hours=hours","sorted_hours[i]
        }

        # 日志类型，用户主动/定时联网
        if(log_type == 1){
            print uuid"$"app_key"$"active_date,uuid,app_key,active_date,hours,time_diff,boottime,version,sdcard_size,rom_size,screen_on,length(battery),log_type,city,region,country,client_ip,visit_times >> "'$data_path'/fact_active-1.tmp"
        }else{
            print uuid"$"app_key,uuid,app_key,active_date,hours,time_diff,boottime,version,sdcard_size,rom_size,screen_on,length(battery),log_type,city,region,country,client_ip,visit_times,0
        }
    }' $data_path/active.tmp > $data_path/fact_active.tmp

    # 主动活跃，获取客户端启动次数
    log_task $LOG_LEVEL_INFO "Get client start times from table: fact_session"
    echo "SELECT
      CONCAT_WS('$', uuid, app_key, create_date),
      COUNT(session_id),
      SUM(duration)
    FROM fact_session
    WHERE create_time < STR_TO_DATE('$run_time','%Y%m%d%H%i%s') $time_filter1
    GROUP BY CONCAT_WS('$', uuid, app_key, create_date);
    " | exec_sql > $data_path/fact_session.tmp
    # 排序、关联
    debug "Sort data: $data_path/fact_active-1.tmp"
    sort $data_path/fact_active-1.tmp -o $data_path/fact_active-1.tmp
    debug "Sort data: $data_path/fact_session.tmp"
    sort $data_path/fact_session.tmp -o $data_path/fact_session.tmp
    debug "Join data: $data_path/fact_active-1.tmp $data_path/fact_session.tmp"
    join -t "$sep" -a 1 $data_path/fact_active-1.tmp $data_path/fact_session.tmp | awk -F '\t' 'BEGIN{OFS=FS}{
        if(NF == 18){
            start_times=0
            duration=0
        }else{
            start_times=$19
            duration=$20
        }
        print $2"$"$3,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,start_times,duration
    }' >> $data_path/fact_active.tmp

    # 获取客户端其他信息（安装渠道号、App初始版本、创建日期）
    log_task $LOG_LEVEL_INFO "Get client other information from table: fact_client"
    echo "SELECT CONCAT(uuid, '$', app_key), customer_id, init_version, create_date FROM fact_client;" | exec_sql > $data_path/fact_client.tmp
    # 排序、关联
    debug "Sort data: $data_path/fact_active.tmp"
    sort $data_path/fact_active.tmp -o $data_path/fact_active.tmp
    debug "Sort data: $data_path/fact_client.tmp"
    sort $data_path/fact_client.tmp -o $data_path/fact_client.tmp
    debug "Join data: $data_path/fact_active.tmp $data_path/fact_client.tmp"
    join -t "$sep" -o 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 1.18 1.19 1.20 2.2 2.3 2.4 $data_path/fact_active.tmp $data_path/fact_client.tmp | mysql_data_conv > $data_path/fact_active.txt

    # 任务重做
    if [[ $redo_flag -eq 1 ]]; then
        log_task $LOG_LEVEL_INFO "Redo task, delete history data"
        echo "DELETE FROM fact_active WHERE 1 = 1 $time_filter2;" | exec_sql
    fi

    # 导入数据
    log_task $LOG_LEVEL_INFO "Load data to table: fact_active"
    echo "LOAD DATA LOCAL INFILE '$data_path/fact_active.txt' INTO TABLE fact_active
    (uuid,app_key,active_date,active_hours,time_diff,boottime,version,sdcard_size,rom_size,screen_on,battery,log_type,city,region,country,client_ip,visit_times,@vstart_times,duration,customer_id,init_version,create_date,date_diff)
    SET start_times=IF(@vstart_times > 0, @vstart_times, 1), date_diff=DATEDIFF(active_date, create_date);
    " | exec_sql
}
execute "$@"