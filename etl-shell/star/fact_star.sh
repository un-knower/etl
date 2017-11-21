#!/bin/bash
# 明星（访问、粉丝、动态、帖子、评论、魅力值、送礼）


source $ETL_HOME/common/db_util.sh


hive_extras="--showHeader=false --silent=true --outputformat=tsv2"
hive_reader="$HIVE_HOME/bin/beeline -u $hive_url -n $hive_user -p $hive_passwd $hive_extras"


function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    # 主页访问
    log_task $LOG_LEVEL_INFO "Get star visit to local file: $data_path/star_visit.tmp"
    sql="DROP TABLE IF EXISTS tmp_star_visit;CREATE TABLE tmp_star_visit AS SELECT acc, COUNT(1) visit_count, COUNT(DISTINCT deviceid) user_count FROM event WHERE \\\`date\\\` = '$prev_day1' AND eventid = 'starinfo' GROUP BY acc;"
    su -l $hive_user -c "hive -e \"$sql\""
    $hive_reader -e "SELECT acc, visit_count, user_count FROM tmp_star_visit;" > $data_path/star_visit.tmp
    sort $data_path/star_visit.tmp -o $data_path/star_visit.tmp

    # 设置数据库连接
    set_db ${JZ_STAR[@]}

    # 新增粉丝
    log_task $LOG_LEVEL_INFO "Get star fans to local file: $data_path/star_fans.tmp"
    echo "SELECT star_id, COUNT(1) FROM t_fan_focus_judge WHERE create_time >= '$prev_day1' AND create_time < '$the_day1' GROUP BY star_id LIMIT 100000000;" | exec_sql > $data_path/star_fans.tmp
    sort $data_path/star_fans.tmp -o $data_path/star_fans.tmp

    # 新增动态
    log_task $LOG_LEVEL_INFO "Get star dynamic to local file: $data_path/star_dynamic.tmp"
    echo "SELECT star_id, dynamic_num FROM t_star_today_dynamic_sns WHERE dynamic_sns_date = '$prev_day1' LIMIT 100000000;" | exec_sql > $data_path/star_dynamic.tmp
    sort $data_path/star_dynamic.tmp -o $data_path/star_dynamic.tmp

    # 新增帖子
    log_task $LOG_LEVEL_INFO "Get star post to local file: $data_path/star_post.tmp"
    echo "SELECT star_id, COUNT(1) FROM t_fan_posts_star WHERE publish_time >= '$prev_day1' AND publish_time < '$the_day1' GROUP BY star_id LIMIT 100000000;" | exec_sql > $data_path/star_post.tmp
    sort $data_path/star_post.tmp -o $data_path/star_post.tmp

    # 新增评论
    log_task $LOG_LEVEL_INFO "Get star comment to local file: $data_path/star_comment.tmp"
    echo "SELECT star_id, COUNT(1) FROM t_fan_comment_today_sns WHERE sns_date = '$prev_day1' GROUP BY star_id LIMIT 100000000;" | exec_sql > $data_path/star_comment.tmp
    sort $data_path/star_comment.tmp -o $data_path/star_comment.tmp

    # 获取明星姓名
    echo "SELECT id, real_name FROM t_interact_star LIMIT 100000000;" | exec_sql > $data_path/star.tmp
    sort $data_path/star.tmp -o $data_path/star.tmp

    # 新增魅力值
    # 新增送礼

    # 关联合并
    log_task $LOG_LEVEL_INFO "Join and merge"
    join -t "$sep" -a1 -e 0 -o 1.1 1.2 1.3 2.2 $data_path/star_visit.tmp $data_path/star_fans.tmp > $data_path/step-1.tmp
    join -t "$sep" -a2 -e 0 -o 2.1 1.2 1.3 2.2 $data_path/star_visit.tmp $data_path/star_fans.tmp > $data_path/step-2.tmp
    cat $data_path/step-1.tmp $data_path/step-2.tmp | sort -u > $data_path/step-3.tmp

    join -t "$sep" -a1 -e 0 -o 1.1 1.2 1.3 1.4 2.2 $data_path/step-3.tmp $data_path/star_dynamic.tmp > $data_path/step-4.tmp
    join -t "$sep" -a2 -e 0 -o 2.1 1.2 1.3 1.4 2.2 $data_path/step-3.tmp $data_path/star_dynamic.tmp > $data_path/step-5.tmp
    cat $data_path/step-4.tmp $data_path/step-5.tmp | sort -u > $data_path/step-6.tmp

    join -t "$sep" -a1 -e 0 -o 1.1 1.2 1.3 1.4 1.5 2.2 $data_path/step-6.tmp $data_path/star_post.tmp > $data_path/step-7.tmp
    join -t "$sep" -a2 -e 0 -o 2.1 1.2 1.3 1.4 1.5 2.2 $data_path/step-6.tmp $data_path/star_post.tmp > $data_path/step-8.tmp
    cat $data_path/step-7.tmp $data_path/step-8.tmp | sort -u > $data_path/step-9.tmp

    join -t "$sep" -a1 -e 0 -o 1.1 1.2 1.3 1.4 1.5 1.6 2.2 $data_path/step-9.tmp $data_path/star_comment.tmp > $data_path/step-10.tmp
    join -t "$sep" -a2 -e 0 -o 2.1 1.2 1.3 1.4 1.5 1.6 2.2 $data_path/step-9.tmp $data_path/star_comment.tmp > $data_path/step-11.tmp
    cat $data_path/step-10.tmp $data_path/step-11.tmp | sort -u > $data_path/step-12.tmp

    join -t "$sep" -a1 -e NULL -o 1.1 2.2 1.2 1.3 1.4 1.5 1.6 1.7 $data_path/step-12.tmp $data_path/star.tmp > $data_path/fact_star.txt

    # 设置数据库连接
    set_db ${FOCUS_DW[@]}

    # 任务重做
    if [[ $redo_flag -eq 1 ]]; then
        log_task $LOG_LEVEL_INFO "Redo task, delete history data"
        echo "DELETE FROM fact_star WHERE create_date = $prev_day;" | exec_sql
    fi

    # 装载数据
    log_task $LOG_LEVEL_INFO "Load data to table: fact_star"
    echo "CREATE TABLE IF NOT EXISTS fact_star (
      star_id bigint,
      real_name varchar(50) COMMENT '真实姓名',
      create_date int COMMENT '创建日期',
      visit_count int COMMENT '访问数',
      user_count int COMMENT '用户数',
      fans_count int COMMENT '粉丝数',
      dynamic_count int COMMENT '动态数',
      post_count int COMMENT '帖子数',
      comment_count int COMMENT '评论数',
      charm_count int COMMENT '魅力值',
      gift_count int COMMENT '礼物数',
      PRIMARY KEY (star_id, create_date)
    ) ENGINE=MyISAM COMMENT='明星';

    LOAD DATA LOCAL INFILE '$data_path/fact_star.txt' INTO TABLE fact_star (star_id, real_name, visit_count, user_count, fans_count, dynamic_count, post_count, comment_count)
    SET create_date='$prev_day';
    " | exec_sql
}
execute "$@"