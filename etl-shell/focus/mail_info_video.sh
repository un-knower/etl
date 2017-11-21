#!/bin/bash
# 资讯视频运营周报表邮件


source $ETL_HOME/common/db_util.sh

function execute()
{
    start_date=`date +'%Y%m%d' -d "$the_day 1 day 2 week ago"`
    end_date=`date +'%Y%m%d' -d "$start_date 6 day"`
    date_range=`date +'%Y%m%d' -d "$start_date"`"-"`date +'%Y%m%d' -d "$end_date"`
    debug "Date range: $date_range"

    if [[ -n "$sub_emails" ]]; then
        # 设置数据库连接
        set_db ${SDK_NDW[@]} "" "" "--column-names"

        echo "SELECT
          CASE b.content_type WHEN 0 THEN '资讯' WHEN 1 THEN '视频' ELSE '其他' END '资源类型',
          '$date_range' '日期范围',
          c.name '类别名称',
          b.title '标题',
          b.source '来源',
          b.publish_time '发布时间',
          b.create_by '创建人',
          IF (b.is_index = 1, '是', '否') '是否推到首页',
          IF (b.push = 1, '是', '否') '是否推送',
          IF (b.source_platform = 1, '自媒体', IF (b.source_platform = 2, '其他', '牙牙资讯')) '来源平台',
          a.*
        FROM
        (
          SELECT
            id,
            SUM(click_count) '点击次数',
            SUM(click_user) '点击人数',
            SUM(comment_count) '评论次数',
            SUM(comment_user) '评论人数',
            SUM(share_count) '分享次数',
            SUM(share_user) '分享人数'
          FROM stat_info
          WHERE stat_date >= $start_date
          AND stat_date <= $end_date
          GROUP BY id
        ) a
        INNER JOIN dim_info b
        INNER JOIN dim_info_cat c
        ON a.id = b.id
        AND b.category_id = c.id;
        " | exec_sql > $data_path/info_video_mail.tmp

        # windows默认编码为gbk
        iconv -f utf-8 -t gbk -c $data_path/info_video_mail.tmp -o $data_path/资讯视频运营周报表.xls

        # 发送
        $SHELL_HOME/common/mail_sender.py "$sub_emails" "资讯视频运营周报表" "数据请查看附件" $data_path/资讯视频运营周报表.xls
    else
        warn "There is no email subscribers"
    fi
}
execute "$@"