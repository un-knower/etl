#!/bin/bash
#
# 粉丝爱心指数Top5


source $ETL_HOME/common/db_util.sh


TOP_COUNT=5


function execute()
{
    export LC_ALL=C

    # 设置数据库连接
    set_db ${JZ_STAR[@]}

    log_task $LOG_LEVEL_INFO "Export data to local file: $data_path/fan_love_all_sns.tmp"
    echo "SELECT fan_id, love_index, create_time, update_time FROM t_public_fan_love_all_sns LIMIT 100000000;" | exec_sql > $data_path/fan_love_all_sns.tmp

    if [[ -s $data_path/fan_love_all_sns.tmp ]]; then
        # 排序
        log_task $LOG_LEVEL_INFO "Sort data and get top $TOP_COUNT"
        sort -t $'\t' -k 2nr -k 4 $data_path/fan_love_all_sns.tmp -o $data_path/fan_love_all_sns.tmp
        head -n $TOP_COUNT $data_path/fan_love_all_sns.tmp > $data_path/fan_love_all_sns.txt

        # 装载数据
        log_task $LOG_LEVEL_INFO "Load data replace into table: t_public_fan_love_all_top"
        echo "TRUNCATE TABLE t_public_fan_love_all_top;
        LOAD DATA LOCAL INFILE '$data_path/fan_love_all_sns.txt' INTO TABLE t_public_fan_love_all_top (fan_id,love_index,create_time,update_time);
        " | exec_sql
    else
        warn "Empty data"
    fi
}
execute "$@"