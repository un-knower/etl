#!/bin/bash
#
# 公益粉丝爱心指数Top20


source $ETL_HOME/common/db_util.sh


TOP_COUNT=20


function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    # 设置数据库连接
    set_db ${JZ_STAR[@]}

    log_task $LOG_LEVEL_INFO "Export data to local file: $data_path/public_fan_love_sns.tmp"
    echo "SELECT public_id, fan_id, love_index, create_time, update_time FROM t_public_fan_love_sns LIMIT 100000000;" | exec_sql > $data_path/public_fan_love_sns.tmp
    sort $data_path/public_fan_love_sns.tmp -o $data_path/public_fan_love_sns.tmp

    # 获取“未开奖”公益
    echo "SELECT id FROM t_star_public WHERE is_draw_lottery = 0 LIMIT 100000000;" | exec_sql > $data_path/public_ids.tmp
    sort $data_path/public_ids.tmp -o $data_path/public_ids.tmp

    # 按公益分类
    rm -f $data_path/public_fan_love_sns_*.tmp
    join -t "$sep" $data_path/public_fan_love_sns.tmp $data_path/public_ids.tmp |
    awk -F '\t' 'BEGIN{OFS=FS}{
        print $0 >> "'$data_path'/public_fan_love_sns_"$1".tmp"
        print $1
    }' | sort -u > $data_path/public_ids.txt

    # 排序取Top
    rm -f $data_path/public_fan_love_sns_rank.txt
    for file in `ls $data_path/public_fan_love_sns_*.tmp`; do
        sort -t $'\t' -k 3nr -k 5 $file -o $file
        head -n $TOP_COUNT $file >> $data_path/public_fan_love_sns_rank.txt
    done

    # 删除公益，装载数据
    if [[ -s $data_path/public_ids.txt ]]; then
        public_ids=`awk '{printf("%s,",$1)}' $data_path/public_ids.txt | sed 's/,$//'`

        echo "DELETE FROM t_public_fan_love_top WHERE public_id IN ($public_ids);
        LOAD DATA LOCAL INFILE '$data_path/public_fan_love_sns_rank.txt' INTO TABLE t_public_fan_love_top (public_id,fan_id,love_index,create_time,update_time);
        " | exec_sql
    fi
}
execute "$@"