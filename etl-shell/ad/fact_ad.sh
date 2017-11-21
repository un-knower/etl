#!/bin/bash
#
# 日志解析
# adv_visitlog -> fact_ad_send fact_ad_show fact_ad_click fact_ad_install
# 可选任务周期：天


source $ETL_HOME/common/db_util.sh


function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    # 初始化取全量数据，之后取前一天数据
    if [[ $is_first -ne 1 ]]; then
        time_filter="AND create_time >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY"
        debug "Got time filter: $time_filter"

        # 任务重做
        time_filter2="AND create_date = $prev_day"
        debug "Got time filter: $time_filter2"
    fi

    # 设置数据库连接
    debug "Set database connection: ${AD_ODS[@]}"
    set_db ${AD_ODS[@]}

    # 获取增量访问日志
    log_task $LOG_LEVEL_INFO "Get incremental access log from table: adv_visitlog"
    echo "SELECT
      androidid,
      DATE_FORMAT(create_time, '%Y%m%d%H'),
      city_id,
      CASE nettype WHEN 1 THEN 2 WHEN 2 THEN 1 else nettype END,
      IF(version > '', version, NULL),
      advids,
      hisshow,
      hisclick,
      hisinstall
    FROM adv_visitlog
    WHERE androidid > '' AND custom_id > '' AND CONCAT(IFNULL(advids, ''), IFNULL(hisshow, ''), IFNULL(hisclick, ''), IFNULL(hisinstall, '')) > ''
    AND create_time < STR_TO_DATE('$run_time','%Y%m%d%H%i%s') $time_filter;
    " | exec_sql > $data_path/visitlog.tmp

    # 解析访问日志得到广告数据
    log_task $LOG_LEVEL_INFO "Parse access log to get ad data"
    rm -f $data_path/send.tmp $data_path/show.tmp $data_path/click.tmp $data_path/install.tmp
    awk -F '\t' 'BEGIN{OFS=FS}{
        split($6,ids,",")
        for(i in ids){
            split(ids[i],arr,"~")
            print arr[1],arr[2],$1,$2,$3,$4,$5 >> "'$data_path'/send.tmp"
        }

        split($7,ids,",")
        for(i in ids){
            print ids[i],$1,$2,$3,$4,$5 >> "'$data_path'/show.tmp"
        }

        split($8,ids,",")
        for(i in ids){
            print ids[i],$1,$2,$3,$4,$5 >> "'$data_path'/click.tmp"
        }

        split($9,ids,",")
        for(i in ids){
            print ids[i],$1,$2,$3,$4,$5 >> "'$data_path'/install.tmp"
        }
    }' $data_path/visitlog.tmp

    # 合并发送
    if [[ -s $data_path/send.tmp ]]; then
        sort -t $'\t' -k 1 -k 3 -k 4 $data_path/send.tmp | awk -F '\t' 'BEGIN{OFS=FS}{
            if($1 == ad_id && $3 == device_id){
                if($5 > 0) city_id=$5
                if($6 > nettype) nettype=$6
                if(version == "NULL") version=$7
                send_count++
            }else{
                if(ad_id != "") print ad_id,res_id,device_id,substr(create_date,1,8),substr(create_date,9,2),city_id,nettype,version,send_count
                ad_id=$1
                res_id=$2
                device_id=$3
                create_date=$4
                city_id=$5
                nettype=$6
                version=$7
                send_count=1
            }
        }END{
            print ad_id,res_id,device_id,substr(create_date,1,8),substr(create_date,9,2),city_id,nettype,version,send_count
        }' > $data_path/fact_ad_send.txt
    else
        touch $data_path/fact_ad_send.txt
    fi

    # 合并展示
    if [[ -s $data_path/show.tmp ]]; then
        sort -t $'\t' -k 1 -k 2 -k 3 $data_path/show.tmp | awk -F '\t' 'BEGIN{OFS=FS}{
            if($1 == res_id && $2 == device_id){
                if($4 > 0) city_id=$4
                if($5 > nettype) nettype=$5
                if(version == "NULL") version=$6
                show_count++
            }else{
                if(res_id != "") print res_id"|"device_id,create_date,city_id,nettype,version,show_count
                res_id=$1
                device_id=$2
                create_date=$3
                city_id=$4
                nettype=$5
                version=$6
                show_count=1
            }
        }END{
            print res_id"|"device_id,create_date,city_id,nettype,version,show_count
        }' | sort -o $data_path/show.txt
    else
        touch $data_path/show.txt
    fi

    # 合并点击
    if [[ -s $data_path/click.tmp ]]; then
        sort -t $'\t' -k 1 -k 2 -k 3 $data_path/click.tmp | awk -F '\t' 'BEGIN{OFS=FS}{
            if($1 == res_id && $2 == device_id){
                if($4 > 0) city_id=$4
                if($5 > nettype) nettype=$5
                if(version == "NULL") version=$6
                click_count++
            }else{
                if(res_id != "") print res_id"|"device_id,create_date,city_id,nettype,version,click_count
                res_id=$1
                device_id=$2
                create_date=$3
                city_id=$4
                nettype=$5
                version=$6
                click_count=1
            }
        }END{
            print res_id"|"device_id,create_date,city_id,nettype,version,click_count
        }' | sort -o $data_path/click.txt
    else
        touch $data_path/click.txt
    fi

    # 合并安装
    if [[ -s $data_path/install.tmp ]]; then
        sort -t $'\t' -k 1 -k 2 -k 3 $data_path/install.tmp | awk -F '\t' 'BEGIN{OFS=FS}{
            if($1 == res_id && $2 == device_id){
                if($4 > 0) city_id=$4
                if($5 > nettype) nettype=$5
                if(version == "NULL") version=$6
                install_count++
            }else{
                if(res_id != "") print res_id"|"device_id,create_date,city_id,nettype,version,install_count
                res_id=$1
                device_id=$2
                create_date=$3
                city_id=$4
                nettype=$5
                version=$6
                install_count=1
            }
        }END{
            print res_id"|"device_id,create_date,city_id,nettype,version,install_count
        }' | sort -o $data_path/install.txt
    else
        touch $data_path/install.txt
    fi

    # 设置数据库连接
    debug "Set database connection: ${AD_DW[@]}"
    set_db ${AD_DW[@]}

    # 任务重做
    if [[ $redo_flag -eq 1 ]]; then
        log_task $LOG_LEVEL_INFO "Redo task, delete history data"
        echo "DELETE FROM fact_ad_send WHERE 1 = 1 $time_filter2;
        DELETE FROM fact_ad_show WHERE 1 = 1 $time_filter2;
        DELETE FROM fact_ad_click WHERE 1 = 1 $time_filter2;
        DELETE FROM fact_ad_install WHERE 1 = 1 $time_filter2;
        " | exec_sql
    fi

    log_task $LOG_LEVEL_INFO "Load data to table: fact_ad_send"
    echo "LOAD DATA LOCAL INFILE '$data_path/fact_ad_send.txt' IGNORE INTO TABLE fact_ad_send (ad_id,res_id,device_id,create_date,create_hour,city_id,nettype,version,send_count);
    " | exec_sql

    echo "SELECT CONCAT_WS('|', res_id, device_id), ad_id, create_date FROM fact_ad_send;" | exec_sql > $data_path/fact_ad_send.txt
    sort $data_path/fact_ad_send.txt -o $data_path/fact_ad_send.txt

    # 关联展示
    join -t "$sep" $data_path/show.txt $data_path/fact_ad_send.txt | awk -F '\t' 'BEGIN{OFS=FS}{
        split($1,arr,"|")
        print $7,arr[2],$8,substr($2,1,8),substr($2,9,2),$3,$4,$5,$6
    }' > $data_path/fact_ad_show.txt

    # 关联点击
    join -t "$sep" $data_path/click.txt $data_path/fact_ad_send.txt| awk -F '\t' 'BEGIN{OFS=FS}{
        split($1,arr,"|")
        print $7,arr[2],$8,substr($2,1,8),substr($2,9,2),$3,$4,$5,$6
    }' > $data_path/fact_ad_click.txt

    # 关联安装
    join -t "$sep" $data_path/install.txt $data_path/fact_ad_send.txt | awk -F '\t' 'BEGIN{OFS=FS}{
        split($1,arr,"|")
        print $7,arr[2],$8,substr($2,1,8),substr($2,9,2),$3,$4,$5,$6
    }' > $data_path/fact_ad_install.txt

    # 导入数据
    log_task $LOG_LEVEL_INFO "Load data to table: fact_ad_show fact_ad_click fact_ad_install"
    echo "LOAD DATA LOCAL INFILE '$data_path/fact_ad_show.txt' INTO TABLE fact_ad_show (ad_id,device_id,send_date,create_date,create_hour,city_id,nettype,version,show_count);
    LOAD DATA LOCAL INFILE '$data_path/fact_ad_click.txt' INTO TABLE fact_ad_click (ad_id,device_id,send_date,create_date,create_hour,city_id,nettype,version,click_count);
    LOAD DATA LOCAL INFILE '$data_path/fact_ad_install.txt' INTO TABLE fact_ad_install (ad_id,device_id,send_date,create_date,create_hour,city_id,nettype,version,install_count);
    " | exec_sql
}
execute "$@"