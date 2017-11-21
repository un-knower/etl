#!/bin/bash
# 互动（点击、分享、评论、参与）


source $ETL_HOME/common/db_util.sh


hive_extras="--showHeader=false --silent=true --outputformat=tsv2"
hive_reader="$HIVE_HOME/bin/beeline -u $hive_url -n $hive_user -p $hive_passwd $hive_extras"


function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    week_num=`date +%w -d "$the_day"`
    week_num=${week_num/0/7}
    week_num=$((week_num - 1))

    start_date=`date +'%Y-%m-%d' -d "$the_day 7 day ago"`
    end_date=`date +'%Y-%m-%d' -d "$the_day 1 day ago"`
    debug "Start date: $start_date, end date: $end_date"

    date_range=`date +'%Y%m%d' -d "$start_date"`"-"`date +'%Y%m%d' -d "$end_date"`
    debug "Date range: $date_range"

    # 设置数据库连接
    set_db ${YAYA_INTERACT[@]}

    # 获取开始时间
    create_date=`echo "SELECT DATE_FORMAT(MIN(create_time), '%Y-%m-%d') FROM t_interact WHERE interact_end_time >= '$start_date' AND interact_end_time < '$end_date' + INTERVAL 1 DAY;" | exec_sql`
    debug "Min create date: $create_date"

    # 上周结束互动
    log_task $LOG_LEVEL_INFO "Get interact to local file: $data_path/interact.tmp"
    echo "SELECT id, title, interact_start_time, interact_end_time FROM t_interact WHERE interact_end_time >= '$start_date' AND interact_end_time < '$end_date' + INTERVAL 1 DAY LIMIT 100000000;" | exec_sql > $data_path/interact.tmp
    sort $data_path/interact.tmp -o $data_path/interact.tmp

    # 点击
    log_task $LOG_LEVEL_INFO "Get interact click to local file: $data_path/interact_click.tmp"
    sql="DROP TABLE IF EXISTS tmp_interact_click;CREATE TABLE tmp_interact_click AS SELECT acc, COUNT(1) click_count, COUNT(DISTINCT deviceid) user_count FROM event WHERE \\\`date\\\` >= '$create_date' AND eventid = 'interact_click' GROUP BY acc;"
    su -l $hive_user -c "hive -e \"$sql\""
    $hive_reader -e "SELECT acc, click_count, user_count FROM tmp_interact_click;" > $data_path/interact_click.tmp
    sort $data_path/interact_click.tmp -o $data_path/interact_click.tmp

    # 分享
    log_task $LOG_LEVEL_INFO "Get interact share to local file: $data_path/interact_share.tmp"
    sql="DROP TABLE IF EXISTS tmp_interact_share;CREATE TABLE tmp_interact_share AS SELECT acc, COUNT(1) share_count FROM event WHERE \\\`date\\\` >= '$create_date' AND eventid = 'share' AND label = '15' GROUP BY acc;"
    su -l $hive_user -c "hive -e \"$sql\""
    $hive_reader -e "SELECT acc, share_count FROM tmp_interact_share;" > $data_path/interact_share.tmp
    sort $data_path/interact_share.tmp -o $data_path/interact_share.tmp

    # 评论
    log_task $LOG_LEVEL_INFO "Get interact comment to local file: $data_path/interact_comment.tmp"
    sql="DROP TABLE IF EXISTS tmp_interact_comment;CREATE TABLE tmp_interact_comment AS SELECT acc, COUNT(1) comment_count FROM event WHERE \\\`date\\\` >= '$create_date' AND eventid = 'comments' AND label = '3' GROUP BY acc;"
    su -l $hive_user -c "hive -e \"$sql\""
    $hive_reader -e "SELECT acc, comment_count FROM tmp_interact_comment;" > $data_path/interact_comment.tmp
    sort $data_path/interact_comment.tmp -o $data_path/interact_comment.tmp

    # 参与

    # 关联合并
    join -t "$sep" -a1 -e 0 -o 1.1 1.2 1.3 1.4 2.2 2.3 $data_path/interact.tmp $data_path/interact_click.tmp | sort > $data_path/step-1.tmp
    join -t "$sep" -a1 -e 0 -o 1.1 1.2 1.3 1.4 1.5 1.6 2.2 $data_path/step-1.tmp $data_path/interact_share.tmp | sort > $data_path/step-2.tmp
    join -t "$sep" -a1 -e 0 -o 1.1 1.2 1.3 1.4 1.5 1.6 1.7 2.2 $data_path/step-2.tmp $data_path/interact_comment.tmp > $data_path/fact_interact.txt

    # 设置数据库连接
    set_db ${FOCUS_DW[@]}

    # 任务重做
    if [[ $redo_flag -eq 1 ]]; then
        log_task $LOG_LEVEL_INFO "Redo task, delete history data"
        echo "DELETE FROM fact_interact WHERE date_range = '$date_range';" | exec_sql
    fi

    # 装载数据
    log_task $LOG_LEVEL_INFO "Load data to table: fact_interact"
    echo "CREATE TABLE IF NOT EXISTS fact_interact (
      id bigint,
      date_range varchar(20) COMMENT '日期范围',
      title varchar(50) COMMENT '标题',
      start_time datetime COMMENT '开始时间',
      end_time datetime COMMENT '结束时间',
      click_count int COMMENT '访问数',
      user_count int COMMENT '用户数',
      share_count int COMMENT '分享数',
      comment_count int COMMENT '评论数',
      join_count int COMMENT '参与数',
      PRIMARY KEY (id, date_range)
    ) ENGINE=MyISAM COMMENT='互动';

    LOAD DATA LOCAL INFILE '$data_path/fact_interact.txt' INTO TABLE fact_interact (id, title, start_time, end_time, click_count, user_count, share_count, comment_count)
    SET date_range='$date_range';
    " | exec_sql

    # 发送邮件
    if [[ -n "$sub_emails" ]]; then
        set_db ${FOCUS_DW[@]} "" "" "--column-names"

        echo "SELECT
          id '互动ID',
          date_range '日期范围',
          title '标题',
          start_time '开始时间',
          end_time '结束时间',
          click_count '点击次数',
          user_count '点击人数',
          share_count '分享次数',
          comment_count '评论次数'
        FROM fact_interact
        WHERE date_range='$date_range';
        " | exec_sql > $data_path/interact_mail.tmp

        # windows默认编码为gbk
        iconv -f utf-8 -t gbk -c $data_path/interact_mail.tmp -o $data_path/互动运营周报表.xls

        # 发送
        $SHELL_HOME/common/mail_sender.py "$sub_emails" "互动运营周报表" "数据请查看附件" $data_path/互动运营周报表.xls
    fi
}
execute "$@"