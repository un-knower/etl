#!/bin/bash
# 明星运营周报表邮件


source $ETL_HOME/common/db_util.sh

function execute()
{
    # 设置数据库连接
    set_db ${SDK_NDW[@]} "" "" "--column-names"

    end_date=$prev_day
    start_date=$(date +%Y%m%d -d "$end_date 6 day ago")

    if [[ -n "$sub_emails" ]]; then
        echo "SELECT
          CONCAT_WS('-', $start_date, $end_date) '日期范围',
          real_name '明星姓名',
          SUM(click_count) '访问次数',
          SUM(click_user) '访问用户数',
          SUM(fans_count) '新增关注',
          SUM(dynamic_num) '新增动态',
          SUM(post_count) '新增帖子',
          SUM(comment_count) '评论数'
        FROM stat_star
        WHERE create_date >= $start_date
        AND create_date <= $end_date
        GROUP BY 1, 2;
        " | exec_sql > $data_path/star_mail.tmp

        # windows默认编码为gbk
        iconv -f utf-8 -t gbk -c $data_path/star_mail.tmp -o $data_path/明星运营周报表.xls

        # 发送
        $SHELL_HOME/common/mail_sender.py "$sub_emails" "明星运营周报表" "数据请查看附件" $data_path/明星运营周报表.xls
    else
        warn "There is no email subscribers"
    fi
}
execute "$@"