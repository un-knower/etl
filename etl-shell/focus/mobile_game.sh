#!/bin/bash
#
# 手游点击数


source $ETL_HOME/common/db_util.sh


function execute()
{
    export LC_ALL=C

    # 设置数据库连接
    set_db ${SDK_ODS[@]}

    # 第一次取全量数据，之后取上一个周期数据
    if [[ $is_first -ne 1 && $redo_flag -ne 1 ]]; then
        time_filter="AND insertdate >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY"
        debug "Got time filter: $time_filter"
    fi

    # 从hdfs下载数据到本地
    log_task $LOG_LEVEL_INFO "Get data from hdfs"
    find $data_path -maxdepth 1 -type f -name "event*" | xargs -r rm -f
    hdfs dfs -get $src_file $data_path

    # 统计数据
    log_task $LOG_LEVEL_INFO "Statistic data to local file: $data_path/mobile_game.tmp"
    cat $data_path/event* | awk -F '\t' '$5 == "game_click" && $9 == 2 {count[$6]++}END{for(key in count){print key,count[key]}}' > $data_path/mobile_game.tmp

    # 去掉已经更新过的
    if [[ -s $data_path/updated_ids.tmp ]]; then
        sort -u $data_path/updated_ids.tmp -o $data_path/updated_ids.tmp
        sort $data_path/mobile_game.tmp -o $data_path/mobile_game.tmp

        join -v 1 $data_path/mobile_game.tmp $data_path/updated_ids.tmp > $data_path/mobile_game.txt
    else
        mv $data_path/mobile_game.tmp $data_path/mobile_game.txt
    fi

    set_db ${JZ_STAR[@]}

    # 更新数据
    while read game_id click_num; do
        echo "UPDATE t_mobile_game_sns SET click_num = click_num + $click_num WHERE game_id = $game_id;" | exec_sql
        echo $game_id >> $data_path/updated_ids.tmp
    done < $data_path/mobile_game.txt
}
execute "$@"