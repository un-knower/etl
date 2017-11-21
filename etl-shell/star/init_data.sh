#!/bin/bash
#
# 数据初始化（以hive用户执行该脚本）


source /etc/profile
source ~/.bash_profile
source $SHELL_HOME/common/include.sh
source $SHELL_HOME/common/db/config.sh
source $SHELL_HOME/common/db/mysql/mysql_util.sh


# 关闭sql日志
SQL_LOG=1


# 粉丝申请圈主判断表 数据初始化
function init_1()
{
    # 关注时间
    debug "Get focus_time from table: t_interact_star_focus"
    echo "SELECT CONCAT_WS('-', star_id, user_id) id, star_id, user_id, create_time FROM t_interact_star_focus GROUP BY id LIMIT 100000000;" | exec_sql | sort > fan_star_focus.tmp

    # 粉丝动态数量
    debug "Get fan_dynamic_num from table: t_fan_star_index"
    echo "SELECT CONCAT_WS('-', star_id, user_id) id, star_id, user_id, FLOOR(dynamic_index / 10) FROM t_fan_star_index GROUP BY id LIMIT 100000000;" | exec_sql | sort > fan_star_index.tmp

    # 粉丝动态置顶数
    debug "Get fan_dynamic_top_num from table: t_star_fan_dynamic"
    echo "SELECT CONCAT_WS('-', star_id, fan_id) id, star_id, fan_id, COUNT(1) FROM t_star_fan_dynamic WHERE is_top = 1 GROUP BY id LIMIT 100000000;" | exec_sql | sort > star_fan_dynamic.tmp

    # 粉丝月排行榜排在1 2 3名的次数
    debug "Get fan_month_num from table: t_fan_month_index_rand"
    rm -f star_month_sns_*.tmp
    echo "SELECT CONCAT_WS('-', star_id, year, month), CONCAT_WS('-', star_id, user_id), total_index, update_time FROM t_fan_month_index_rand LIMIT 100000000;" | exec_sql |
    awk '{
        print $0 >> "star_month_sns_"$1".tmp"
    }'
    for file in `ls star_month_sns_*.tmp`; do
        sort -t $'\t' -k 3nr -k 4 $file | head -n 3
    done | awk -F '\t' 'BEGIN{OFS=FS}{
        sum[$2]++
    }END{
        for(key in sum){
            split(key,arr,"-")
            print key,arr[1],arr[2],sum[key]
        }
    }' | sort > star_month_sns.tmp

    # 合并 关注时间 粉丝动态数量
    debug "Merge focus_time with fan_dynamic_num"
    join -t "$sep" -a1 -e NULL -o 1.1 1.2 1.3 1.4 2.4 fan_star_focus.tmp fan_star_index.tmp > step-1.tmp
    join -t "$sep" -a2 -e NULL -o 2.1 2.2 2.3 1.4 2.4 fan_star_focus.tmp fan_star_index.tmp > step-2.tmp
    cat step-1.tmp step-2.tmp | sort -u > step-3.tmp

    # 合并 粉丝动态置顶数
    debug "Merge fan_dynamic_top_num"
    join -t "$sep" -a1 -e NULL -o 1.1 1.2 1.3 1.4 1.5 2.4 step-3.tmp star_fan_dynamic.tmp > step-4.tmp
    join -t "$sep" -a2 -e NULL -o 2.1 2.2 2.3 1.4 1.5 2.4 step-3.tmp star_fan_dynamic.tmp > step-5.tmp
    cat step-4.tmp step-5.tmp | sort -u > step-6.tmp

    # 合并 粉丝月排行榜排在1 2 3名的次数
    debug "Merge fan_month_num"
    join -t "$sep" -a1 -e NULL -o 1.2 1.3 1.4 1.5 1.6 2.4 step-6.tmp star_month_sns.tmp > step-7.tmp
    join -t "$sep" -a2 -e NULL -o 2.2 2.3 1.4 1.5 1.6 2.4 step-6.tmp star_month_sns.tmp > step-8.tmp
    cat step-7.tmp step-8.tmp | sort -u > result.txt

    # 装载入库
    debug "Load data into table: t_fan_apply_header_filter_record"
    echo "truncate table t_fan_apply_header_filter_record;
    LOAD DATA LOCAL INFILE 'result.txt' INTO TABLE t_fan_apply_header_filter_record (star_id, user_id, focus_time, fan_dynamic_num, fan_dynamic_top_num, fan_month_num)
    SET fan_dynamic_cream_num=0, create_time=NOW(), update_time=NOW();
    UPDATE t_fan_apply_header_filter_record SET focus_time = NULL WHERE focus_time = 0;
    " | exec_sql

    # 删除临时数据
    rm -f star_month_sns_*.tmp
    #rm -f fan_star_focus.tmp
    #rm -f fan_star_index.tmp
    #rm -f star_fan_dynamic.tmp
    #rm -f star_month_sns.tmp
    #rm -f step-*.tmp
}

# 粉丝-明星关注池 数据初始化
function init_2()
{
    echo "SELECT star_id,
      user_id,
      create_time,
      1 focus_status,
      create_time update_time
    FROM t_interact_star_focus
    LIMIT 100000000;
    " | exec_sql > fan_star_focus.txt

    echo "CREATE TABLE IF NOT EXISTS fan_star_focus(
      star_id BIGINT,
      user_id BIGINT,
      create_time TIMESTAMP COMMENT '首次关注时间',
      focus_status TINYINT COMMENT '当前关注状态 1:关注中 0:取消关注',
      update_time TIMESTAMP COMMENT '关注状态更新时间'
    ) COMMENT '粉丝-明星关注池'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

    TRUNCATE TABLE fan_star_focus;
    LOAD DATA LOCAL INPATH 'fan_star_focus.txt' INTO TABLE fan_star_focus;
    " | hive -S
}

# 粉丝-明星动态池 数据初始化
function init_3()
{
    # 明星动态
    echo "SELECT star_id,
      NULL user_id,
      publish_time,
      id dynamic_id,
      is_top,
      0 is_cream,
      1 dynamic_type,
      update_time
    FROM t_interact_star_dynamic
    WHERE is_delete = 0
    AND is_online = 1
    LIMIT 100000000;
    " | exec_sql | sed 's/^NULL\t/\\N\t/g;s/\tNULL$/\t\\N/g;s/\tNULL\t/\t\\N\t/g;s/\tNULL\t/\t\\N\t/g' > star_dynamic.txt

    # 粉丝动态
    echo "SELECT star_id,
      fan_id,
      publish_time,
      id dynamic_id,
      is_top,
      IFNULL(is_cream, 0),
      2 dynamic_type,
      update_time
    FROM t_fan_dynamic
    WHERE (is_need_audit = 0 OR (is_need_audit = 1 AND is_audit = 2 AND audit_score = 2))
    AND is_delete = 0
    LIMIT 100000000;
    " | exec_sql > fan_dynamic.txt

    echo "CREATE TABLE IF NOT EXISTS fan_star_dynamic(
      star_id BIGINT,
      user_id BIGINT,
      create_time TIMESTAMP COMMENT '动态发布时间',
      dynamic_id BIGINT COMMENT '动态ID',
      is_top TINYINT COMMENT '是否置顶 1:是 0:否',
      is_cream TINYINT COMMENT '是否加精 1:是 0:否',
      dynamic_type TINYINT COMMENT '动态类型 1:明星动态 2:粉丝动态',
      update_time TIMESTAMP COMMENT '最后更新时间'
    ) COMMENT '粉丝-明星动态池'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

    TRUNCATE TABLE fan_star_dynamic;
    LOAD DATA LOCAL INPATH 'star_dynamic.txt' INTO TABLE fan_star_dynamic;
    LOAD DATA LOCAL INPATH 'fan_dynamic.txt' INTO TABLE fan_star_dynamic;
    " | hive -S
}

# 关注分数更新
function init_4()
{
    # 更新默认值
    echo "UPDATE t_star_month_sns SET sofan_score = 0;
    UPDATE t_star_month_sns SET fan_dynamic_score = 0;
    UPDATE t_star_month_sns SET focus_score = fan_num;
    UPDATE t_star_month_sns SET gift_score = gift_num;
    UPDATE t_star_month_sns SET total_score = sofan_score + fan_dynamic_score + focus_score + gift_score;

    UPDATE t_star_sns SET sofan_score = 0;
    UPDATE t_star_sns SET fan_dynamic_score = 0;
    UPDATE t_star_sns SET focus_score = fan_num;
    UPDATE t_star_sns SET gift_score = gift_num;
    UPDATE t_star_sns SET total_score = sofan_score + fan_dynamic_score + focus_score + gift_score;

    UPDATE t_fan_day_index SET fan_dynamic_score = 0, comment_score = 0, hot_comment_score = 0, focus_score = 0, post_good_score = 0, pull_ring_index = 0, dynamic_index = 0, gift_index = 0, total_index = 0;
    UPDATE t_fan_month_index SET fan_dynamic_score = dynamic_index, comment_score = 0, hot_comment_score = 0, focus_score = 0, post_good_score = 0;
    UPDATE t_fan_star_index SET fan_dynamic_score = dynamic_index, comment_score = 0, hot_comment_score = 0, focus_score = 0, post_good_score = 0;
    UPDATE t_fan_total_index SET fan_dynamic_score = dynamic_index, comment_score = 0, hot_comment_score = 0, focus_score = 0, post_good_score = 0;
    " | exec_sql

    # 粉丝关注总分数
    echo "SELECT user_id, SUM(50) FROM t_interact_star_focus GROUP BY user_id LIMIT 100000000;" | exec_sql | awk '{
        printf("UPDATE t_fan_total_index SET focus_score = %d WHERE user_id = %d;\n",$2,$1)
    }END{
        print "UPDATE t_fan_total_index SET total_index = post_good_score + focus_score + hot_comment_score + comment_score + fan_dynamic_score + gift_index + pull_ring_index;"
    }' | exec_sql

    # 粉丝关注明星分数
    echo "SELECT user_id, star_id, 50 FROM t_interact_star_focus LIMIT 100000000;" | exec_sql | awk '{
        printf("UPDATE t_fan_star_index SET focus_score = %d WHERE user_id = %d AND star_id = %d;\n",$3,$1,$2)
    }END{
        print "UPDATE t_fan_star_index SET total_index = post_good_score + focus_score + hot_comment_score + comment_score + fan_dynamic_score + gift_index + pull_ring_index;"
    }' | exec_sql

    # 粉丝月指数
    echo "SELECT user_id, star_id, YEAR(create_time), MONTH(create_time), 50 FROM t_interact_star_focus LIMIT 100000000;" | exec_sql | awk '{
        printf("UPDATE t_fan_month_index SET focus_score = %d WHERE user_id = %d AND star_id = %d AND year = %d AND month = %d;\n",$5,$1,$2,$3,$4)
    }END{
        print "UPDATE t_fan_month_index SET total_index = post_good_score + focus_score + hot_comment_score + comment_score + fan_dynamic_score + gift_index + pull_ring_index;"
    }' | exec_sql

    # 粉丝关注判断表
    echo "SELECT user_id, star_id, create_time FROM t_interact_star_focus LIMIT 100000000;" | exec_sql > fan_focus_judge.txt
    echo "truncate table t_fan_focus_judge;
    LOAD DATA LOCAL INFILE 'fan_focus_judge.txt' INTO TABLE t_fan_focus_judge (user_id, star_id, create_time);
    " | exec_sql
}

function main()
{
    run_env="$1"

    source $ETL_HOME/common/db_util.sh

    export LC_ALL=C
    sep=`echo -e "\t"`

    # 设置数据库连接
    set_db ${JZ_STAR[@]}

    init_4
    init_1
    init_2
    init_3
}
main "$@"