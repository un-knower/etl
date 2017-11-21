#!/bin/bash
#
# 明星粉丝指数Top10


source $ETL_HOME/common/db_util.sh


TOP_COUNT=10


function execute()
{
    export LC_ALL=C

    # 设置数据库连接
    set_db ${JZ_STAR[@]}

    log_task $LOG_LEVEL_INFO "Export data to local file: $data_path/fan_index.tmp"
    echo "SELECT star_id, user_id, total_index, create_time, update_time, fan_dynamic_score, comment_score, hot_comment_score, focus_score, post_good_score
    FROM t_fan_star_index
    LIMIT 100000000;
    " | exec_sql > $data_path/fan_index.tmp

    if [[ -s $data_path/fan_index.tmp ]]; then
        # 按明星分类
        debug "Classify fans according to star"
        rm -f $data_path/fan_index_*.tmp
        awk -F '\t' 'BEGIN{OFS=FS}{
            print $0 >> "'$data_path'/fan_index_"$1".tmp"
        }' $data_path/fan_index.tmp

        # 排序
        debug "Sort fans according to fan index and get top $TOP_COUNT"
        rm -f $data_path/fan_index_rank.txt
        for file in `ls $data_path/fan_index_*.tmp`; do
            sort -t $'\t' -k 3nr -k 5 $file -o $file
            head -n $TOP_COUNT $file | awk 'BEGIN{OFS="\t"}{print $0,++i}' >> $data_path/fan_index_rank.txt
        done

        # 装载数据
        debug "Load data to table: t_star_fans_index_top10"
        echo "TRUNCATE TABLE t_star_fans_index_top10;
        LOAD DATA LOCAL INFILE '$data_path/fan_index_rank.txt' INTO TABLE t_star_fans_index_top10 (star_id,fan_id,total_index,create_time,update_time,fan_dynamic_score,comment_score,hot_comment_score,focus_score,post_good_score,topn);
        " | exec_sql
    else
        warn "Empty data"
    fi
}
execute "$@"