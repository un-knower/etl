#!/bin/bash
# 资讯（点击、分享、评论）


source $ETL_HOME/common/db_util.sh


hive_extras="--showHeader=false --silent=true --outputformat=tsv2"
hive_reader="$HIVE_HOME/bin/beeline -u $hive_url -n $hive_user -p $hive_passwd $hive_extras"


function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    start_date=`date +'%Y-%m-%d' -d "$the_day 1 day 2 week ago"`
    end_date=`date +'%Y-%m-%d' -d "$start_date 6 day"`
    debug "Start date: $start_date, end date: $end_date"

    date_range=`date +'%Y%m%d' -d "$start_date"`"-"`date +'%Y%m%d' -d "$end_date"`
    debug "Date range: $date_range"

    # 点击
    log_task $LOG_LEVEL_INFO "Get info click to local file: $data_path/info_click.tmp"
    sql="DROP TABLE IF EXISTS tmp_info_click;CREATE TABLE tmp_info_click AS SELECT acc, COUNT(1) click_count, COUNT(DISTINCT deviceid) user_count FROM event WHERE \\\`date\\\` >= '$start_date' AND eventid = 'recreate_click' GROUP BY acc;"
    su -l $hive_user -c "hive -e \"$sql\""
    $hive_reader -e "SELECT acc, click_count, user_count FROM tmp_info_click;" > $data_path/info_click.tmp
    sort $data_path/info_click.tmp -o $data_path/info_click.tmp

    # 分享
    log_task $LOG_LEVEL_INFO "Get info share to local file: $data_path/info_share.tmp"
    sql="DROP TABLE IF EXISTS tmp_info_share;CREATE TABLE tmp_info_share AS SELECT acc, COUNT(1) share_count FROM event WHERE \\\`date\\\` >= '$start_date' AND eventid = 'share' AND label = '1' GROUP BY acc;"
    su -l $hive_user -c "hive -e \"$sql\""
    $hive_reader -e "SELECT acc, share_count FROM tmp_info_share;" > $data_path/info_share.tmp
    sort $data_path/info_share.tmp -o $data_path/info_share.tmp

    # 评论
    log_task $LOG_LEVEL_INFO "Get info comment to local file: $data_path/info_comment.tmp"
    sql="DROP TABLE IF EXISTS tmp_info_comment;CREATE TABLE tmp_info_comment AS SELECT acc, COUNT(1) comment_count FROM event WHERE \\\`date\\\` >= '$start_date' AND eventid = 'comments' AND label = '1' GROUP BY acc;"
    su -l $hive_user -c "hive -e \"$sql\""
    $hive_reader -e "SELECT acc, comment_count FROM tmp_info_comment;" > $data_path/info_comment.tmp
    sort $data_path/info_comment.tmp -o $data_path/info_comment.tmp

    # 设置数据库连接
    set_db ${JZ_YAYA[@]}

    # 资讯
    log_task $LOG_LEVEL_INFO "Get info to local file: $data_path/info.tmp"
    echo "SELECT
      id,
      category_name,
      title,
      source,
      publish_time,
      create_by,
      is_index,
      push,
      source_platform
    FROM t_information
    WHERE content_type = 0
    AND publish_time >= '$start_date'
    AND publish_time < '$end_date' + INTERVAL 1 DAY
    LIMIT 100000000;
    " | exec_sql > $data_path/info.tmp
    sort $data_path/info.tmp -o $data_path/info.tmp

    # 关联
    join -t "$sep" -a 1 -e 0 -o 1.1 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 2.2 2.3 $data_path/info.tmp $data_path/info_click.tmp | sort > $data_path/step-1.tmp
    join -t "$sep" -a 1 -e 0 -o 1.1 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 2.2 $data_path/step-1.tmp $data_path/info_share.tmp | sort > $data_path/step-2.tmp
    join -t "$sep" -a 1 -e 0 -o 1.1 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 2.2 $data_path/step-2.tmp $data_path/info_comment.tmp > $data_path/fact_info.txt

    # 设置数据库连接
    set_db ${FOCUS_DW[@]}

    # 任务重做
    if [[ $redo_flag -eq 1 ]]; then
        log_task $LOG_LEVEL_INFO "Redo task, delete history data"
        echo "DELETE FROM fact_info WHERE date_range = '$date_range';" | exec_sql
    fi

    # 装载
    log_task $LOG_LEVEL_INFO "Load data to table: fact_info"
    echo "CREATE TABLE IF NOT EXISTS fact_info (
      id bigint,
      date_range varchar(20) COMMENT '日期范围',
      category_name varchar(128) COMMENT '类别名称',
      title varchar(128) NOT NULL DEFAULT '' COMMENT '标题',
      source varchar(64) COMMENT '来源',
      publish_time datetime COMMENT '发布时间',
      create_by varchar(32) COMMENT '创建人',
      is_index tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否推到首页，0否，1是',
      is_push int(1) NOT NULL DEFAULT '0' COMMENT '是否推送， 0表示未推送， 1表示已经推送',
      source_platform int COMMENT '来源平台，0牙牙资讯，1自媒体，2其他',
      click_count int COMMENT '点击数',
      user_count int COMMENT '点击人数',
      share_count int COMMENT '分享数',
      comment_count int COMMENT '评论数',
      PRIMARY KEY (id, date_range)
    ) ENGINE=MyISAM COMMENT='资讯';

    LOAD DATA LOCAL INFILE '$data_path/fact_info.txt' INTO TABLE fact_info (id, category_name, title, source, publish_time, create_by, is_index, is_push, source_platform, click_count, user_count, share_count, comment_count)
    SET date_range='$date_range';
    " | exec_sql

    # 发送邮件
    if [[ -n "$sub_emails" ]]; then
        set_db ${FOCUS_DW[@]} "" "" "--column-names"

        echo "SELECT
          id '资讯ID',
          date_range '日期范围',
          category_name '类别名称',
          title '标题',
          source '来源',
          publish_time '发布时间',
          create_by '创建人',
          IF(is_index = 1, '是', '否') '是否推到首页',
          IF(is_push = 1, '是', '否') '是否推送',
          IF(source_platform = 1, '自媒体', IF(source_platform = 2, '其他', '牙牙资讯')) '来源平台',
          click_count '点击次数',
          user_count '点击人数',
          share_count '分享次数',
          comment_count '评论次数'
        FROM fact_info
        WHERE date_range='$date_range';
        " | exec_sql > $data_path/info_mail.tmp

        # windows默认编码为gbk
        iconv -f utf-8 -t gbk -c $data_path/info_mail.tmp -o $data_path/资讯运营周报表.xls

        # 发送
        $SHELL_HOME/common/mail_sender.py "$sub_emails" "资讯运营周报表" "数据请查看附件" $data_path/资讯运营周报表.xls
    fi
}
execute "$@"