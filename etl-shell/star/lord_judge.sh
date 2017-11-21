#!/bin/bash
#
# 粉丝申请圈主判断


source $ETL_HOME/common/db_util.sh


hive_extras="--showHeader=false --silent=true --outputformat=tsv2"
hive_reader="$HIVE_HOME/bin/beeline -u $hive_url -n $hive_user -p $hive_passwd $hive_extras"

# 粉丝月排行榜排top数
FAN_TOP_COUNT=3

# 粉丝关注明星天数
FAN_FOCUS_DAYS=30

# 粉丝在粉圈发布动态数
FAN_DYNAMIC_NUM=30

# 粉丝在粉圈发布动态被置顶/加精数
FAN_TOPCREAM_NUM=2

# 粉丝月排行榜排在1 2 3名的次数
FAN_TOP_TIMES=1


function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    # 设置数据库连接
    set_db ${JZ_STAR[@]}

    # 粉丝最近关注此明星时间
    info "Get fans focus time"
    sql="SELECT STARID, ACCID, UPDATETIME FROM FOCUS WHERE STARID != 'null' AND ACCID != 'null' AND UPDATETIME >= '$prev_day1' AND UPDATETIME < '$the_day1'"
    $JAVA_HOME/bin/java -cp $phoenix_classpath $phoenix_reader $zookeeper_url "$sql" | awk -F '\t' '{
        printf("INSERT INTO t_fan_apply_header_filter_record (star_id, user_id, focus_time) VALUES (%s, %s, \"%s\") ON DUPLICATE KEY UPDATE focus_time = \"%s\";\n",$1,$2,$3,$3)
    }' | exec_sql

    # 粉丝动态数 粉丝动态置顶数 粉丝动态加精数
    info "Get fans dynamic topped dynamic cream dynamic"
    $hive_reader -e "SELECT starId, userId, dnum, topnum, creamnum FROM fans_dynamic_info WHERE starId != 'null' AND userId != 'null' AND \`date\` = '$prev_day1';" | awk -F '\t' '{
        printf("INSERT INTO t_fan_apply_header_filter_record (star_id, user_id, fan_dynamic_num, fan_dynamic_top_num, fan_dynamic_cream_num) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE fan_dynamic_num = %s, fan_dynamic_top_num = %s, fan_dynamic_cream_num = %s;\n",$1,$2,$3,$4,$5,$3,$4,$5)
    }' | exec_sql

    # 粉丝月排行榜排在1 2 3名的次数
    info "Get fans month top times"
    rm -f $data_path/star_month_sns_*.tmp
    echo "SELECT CONCAT_WS('-', star_id, year, month),
      CONCAT_WS('-', star_id, user_id),
      total_index,
      update_time
    FROM t_fan_month_index_rand
    LIMIT 100000000;
    " | exec_sql | awk '{
        print $0 >> "'$data_path'/star_month_sns_"$1".tmp"
    }'
    for file in `ls $data_path/star_month_sns_*.tmp`; do
        sort -t $'\t' -k 3nr -k 4 $file | head -n $FAN_TOP_COUNT
    done | awk -F '\t' 'BEGIN{OFS=FS}{
        sum[$2]++
    }END{
        for(key in sum){
            split(key,arr,"-")
            print arr[1],arr[2],sum[key]
        }
    }' | awk -F '\t' '{
        printf("INSERT INTO t_fan_apply_header_filter_record (star_id, user_id, fan_month_num) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE fan_month_num = %s;\n",$1,$2,$3,$3)
    }' | exec_sql

    # 装载数据
    info "Update data"
    echo "UPDATE t_fan_apply_header_filter_record SET create_time = NOW() WHERE create_time = 0;
    UPDATE t_fan_apply_header_filter_record SET update_time = create_time WHERE update_time = 0;
    UPDATE t_fan_apply_header_filter_record SET create_by = NULL WHERE create_by = 0;
    UPDATE t_fan_apply_header_filter_record SET focus_time = NULL WHERE focus_time = 0;

    UPDATE t_fan_apply_header_filter_record
    SET is_satisfy_header = 1
    WHERE focus_time < CURDATE() - INTERVAL $FAN_FOCUS_DAYS DAY
    AND fan_dynamic_num >= $FAN_DYNAMIC_NUM
    AND (fan_dynamic_top_num + fan_dynamic_cream_num) >= $FAN_TOPCREAM_NUM
    AND fan_month_num >= $FAN_TOP_TIMES;
    " | exec_sql
}
execute "$@"