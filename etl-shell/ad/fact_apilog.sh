#!/bin/bash
#
# 日志解析
# adv_apilog -> fact_apilog
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
    fi

    # 设置数据库连接
    debug "Set database connection: ${AD_ODS[@]}"
    set_db ${AD_ODS[@]}

    # 获取增量访问日志
    log_task $LOG_LEVEL_INFO "Get incremental api log from table: adv_apilog"
    rm -f $data_path/send.tmp $data_path/show.tmp $data_path/click.tmp
    echo "SELECT
      androidid,
      IFNULL(ids, ''),
      IFNULL(apishow, ''),
      IFNULL(apiclick, ''),
      DATE_FORMAT(create_time, '%Y%m%d'),
      apiurl,
      clickurl,
      custom_id,
      imei,
      title,
      adtype
    FROM adv_apilog
    WHERE androidid > '' AND custom_id > '' AND CONCAT(IFNULL(ids, ''), IFNULL(apiclick, ''), IFNULL(apishow, '')) > ''
    AND create_time < STR_TO_DATE('$run_time','%Y%m%d%H%i%s') $time_filter;
    " | exec_sql | awk -F '\t' 'BEGIN{OFS=FS}{
        if($2 > "") print $1"|"$2,$5,$6,$7,$8,$9,$10,$11 >> "'$data_path'/send.tmp"

        split($3,ids,",")
        for(i in ids){
            print $1"|"ids[i],$5 >> "'$data_path'/show.tmp"
        }

        split($4,ids,",")
        for(i in ids){
            print $1"|"ids[i],$5 >> "'$data_path'/click.tmp"
        }
    }'

    # 设置数据库连接
    debug "Set database connection: ${AD_DW[@]}"
    set_db ${AD_DW[@]}

    # 获取历史数据
    echo "SELECT
      CONCAT_WS('|', androidid, ad_id),
      create_date,
      apiurl,
      clickurl,
      custom_id,
      imei,
      title,
      adtype
    FROM fact_apilog;
    " | exec_sql >>  $data_path/send.tmp

    echo "SELECT CONCAT_WS('|', androidid, ad_id), show_date FROM fact_apilog WHERE ad_status > 1" | exec_sql >> $data_path/show.tmp

    echo "SELECT CONCAT_WS('|', androidid, ad_id), click_date FROM fact_apilog WHERE ad_status > 2" | exec_sql >> $data_path/click.tmp

    # 排序去重
    sort -u $data_path/send.tmp -o $data_path/send.tmp
    sort -u $data_path/show.tmp -o $data_path/show.tmp
    sort -u $data_path/click.tmp -o $data_path/click.tmp

    # 关联
    join -t "$sep" -a1 $data_path/show.tmp $data_path/click.tmp | awk -F '\t' 'BEGIN{OFS=FS}{
        if(NF == 3){
            print $0,NF
        }else{
            print $0,"NULL",NF
        }
    }' | sort > $data_path/show_click.tmp

    join -t "$sep" -a1 $data_path/send.tmp $data_path/show_click.tmp | awk -F '\t' 'BEGIN{OFS=FS}{
        ad_status=$NF
        if(NF == 8){
            ad_status=1
            $9="NULL"
            $10="NULL"
        }
        split($1,arr,"|")
        print arr[1],arr[2],$3,$4,$5,$6,$7,$8,ad_status,$2,$9,$10
    }' > $data_path/fact_apilog.txt

    # 导入数据
    log_task $LOG_LEVEL_INFO "Load data to table: fact_apilog"
    echo "LOAD DATA LOCAL INFILE '$data_path/fact_apilog.txt' REPLACE INTO TABLE fact_apilog (androidid,ad_id,apiurl,clickurl,custom_id,imei,title,adtype,ad_status,create_date,show_date,click_date);
    " | exec_sql
}
execute "$@"