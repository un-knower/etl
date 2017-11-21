#!/bin/bash
# 明星运营周报表邮件


source $ETL_HOME/common/db_util.sh

function execute()
{
    start_date=`date +'%Y-%m-%d' -d "$the_day 7 day ago"`
    end_date=`date +'%Y-%m-%d' -d "$the_day 1 day ago"`
    date_range=`date +'%Y%m%d' -d "$start_date"`"-"`date +'%Y%m%d' -d "$end_date"`
    debug "Date range: $date_range"

    if [[ -n "$sub_emails" ]]; then
        # 设置数据库连接
        set_db ${SDK_NDW[@]} "" "" "--column-names"

        echo "SELECT
          id,
          date_range '日期范围',
          title '标题',
          start_time '开始时间',
          end_time '结束时间',
          click_count '点击次数',
          click_user '点击人数',
          comment_count '评论次数',
          comment_user '评论人数',
          share_count '分享次数',
          share_user '分享人数'
        FROM stat_interact
        WHERE date_range='$date_range';
        " | exec_sql > $data_path/interact_mail.tmp

        # windows默认编码为gbk
        iconv -f utf-8 -t gbk -c $data_path/interact_mail.tmp -o $data_path/互动运营周报表.xls

        # 发送
        $SHELL_HOME/common/mail_sender.py "$sub_emails" "互动运营周报表" "数据请查看附件" $data_path/互动运营周报表.xls
    else
        warn "There is no email subscribers"
    fi
}
execute "$@"