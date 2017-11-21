#!/bin/bash
# 资讯视频（点击、评论、点赞、分享）


source /etc/profile
source ~/.bash_profile


hive_url=jdbc:hive2://10.10.20.101:10000/default
hive_user=hive
hive_passwd=hive123
hive_extras="--showHeader=false --silent=true --outputformat=tsv2"
hive_reader="$HIVE_HOME/bin/beeline -u $hive_url -n $hive_user -p $hive_passwd $hive_extras"


function main()
{
    if [[ $# -lt 2 ]]; then
        echo "Usage: $0 <start date> <end date>"
        echo "Example: $0 2016-08-15 2016-08-21"
        exit 1
    fi

    start_date="$1"
    end_date="$2"

    set -e

    export LC_ALL=C
    sep=`echo -e "\t"`

    date_range=`date +'%Y%m%d' -d "$start_date"`"-"`date +'%Y%m%d' -d "$end_date"`

    # 资讯视频点击
    echo "DROP TABLE IF EXISTS info_video_click;
    CREATE TABLE info_video_click AS SELECT
      eventid,
      acc,
      COUNT(1) click_count,
      COUNT(DISTINCT deviceid) user_count
    FROM event
    WHERE insertdate >= '$start_date'
    AND eventid IN ('recreate_click', 'video_click')
    GROUP BY eventid, acc;
    " | hive -S

    sql="SELECT CONCAT(CASE eventid WHEN 'recreate_click' THEN 1 WHEN 'video_click' THEN 2 ELSE 0 END, '-', acc) id, click_count, user_count FROM info_video_click;"
    $hive_reader -e "$sql" | sort > info_video_click.tmp

    # 资讯视频分享
    echo "DROP TABLE IF EXISTS info_video_share;
    CREATE TABLE info_video_share AS SELECT
      label,
      acc,
      COUNT(1) share_count
    FROM event
    WHERE insertdate >= '$start_date'
    AND eventid = 'share'
    AND label IN ('1', '2')
    GROUP BY label, acc;
    " | hive -S

    sql="SELECT CONCAT(label, '-', acc), share_count FROM info_video_share;"
    $hive_reader -e "$sql" | sort > info_video_share.tmp

    # 资讯发布
    echo "SELECT
      CONCAT('1-', a.id),
      a.source,
      a.title,
      a.publish_time,
      a.create_by,
      IFNULL(b.comment_count, 0),
      IFNULL(b.praise_count, 0)
    FROM (
      SELECT id, source, title, publish_time, create_by FROM t_information WHERE publish_time >= '$start_date' AND publish_time < '$end_date'
    ) a
    LEFT JOIN t_bussiness_sns b
    ON a.id = b.bussiness_id
    AND b.model_id = 1;
    " | mysql -h10.10.10.245 -uwdh -pkQj43dgqJtbOYi -P3308 jz_yaya -s -N --local-infile > info.tmp

    # 视频发布
    echo "SELECT
      CONCAT('2-', a.id),
      NULL,
      a.title,
      a.create_time,
      a.create_by,
      IFNULL(b.comment_count, 0),
      IFNULL(b.praise_count, 0)
    FROM (
      SELECT id, title, create_time, create_by FROM t_video_info WHERE create_time >= '$start_date' AND create_time < '$end_date'
    ) a
    LEFT JOIN t_bussiness_sns b
    ON a.id = b.bussiness_id
    AND b.model_id = 2;
    " | mysql -h10.10.10.245 -uwdh -pkQj43dgqJtbOYi -P3308 jz_yaya -s -N --local-infile > video.tmp

    # 合并资讯视频
    cat info.tmp video.tmp | sort > info_video.tmp

    # 关联
    join -t "$sep" -a 1 -e 0 -o 1.1 1.2 1.3 1.4 1.5 2.2 2.3 1.6 1.7 info_video.tmp info_video_click.tmp | sort > step-1.tmp
    join -t "$sep" -a 1 -e 0 -o 1.1 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 2.2 step-1.tmp info_video_share.tmp > info_video.txt

    # 装载
    echo "SET NAMES utf8;
    CREATE TABLE IF NOT EXISTS fact_info_video (
      id bigint,
      model_id int COMMENT '模块ID：1=娱乐 2=视频',
      date_range varchar(20) COMMENT '日期范围',
      source varchar(64) COMMENT '来源',
      title varchar(128) COMMENT '标题',
      publish_time datetime COMMENT '发布时间',
      create_by varchar(32) COMMENT '创建人',
      click_count int COMMENT '点击数',
      user_count int COMMENT '点击人数',
      comment_count int COMMENT '评论数',
      praise_count int COMMENT '点赞数',
      share_count int COMMENT '分享数',
      PRIMARY KEY (id, model_id, date_range)
    ) ENGINE=MyISAM COMMENT='资讯视频';

    DELETE FROM fact_info_video WHERE date_range = '$date_range';
    LOAD DATA LOCAL INFILE 'info_video.txt' INTO TABLE fact_info_video (@ids, source, title, publish_time, create_by, click_count, user_count, comment_count, praise_count, share_count)
    SET id=SUBSTR(@ids, POSITION('-' IN @ids) + 1), model_id=SUBSTR(@ids, 1, POSITION('-' IN @ids) - 1), date_range='$date_range';
    " | mysql -h10.10.10.171 -uroot -pZv7oBWPoiOyLOwoh focus_dw -s -N --local-infile
}
main "$@"