#!/bin/bash
#
# 初始化维度


source $SHELL_HOME/common/include.sh
source $SHELL_HOME/common/date_util.sh
source $SHELL_HOME/common/db/config.sh
source $SHELL_HOME/common/db/mysql/mysql_util.sh
source $ETL_HOME/common/db_util.sh


# 日期维度
function dim_date()
{
    echo "DROP TABLE IF EXISTS dim_date;
    CREATE TABLE dim_date (
      id INT,
      the_year SMALLINT,
      half_year TINYINT,
      the_quarter TINYINT,
      month_of_year TINYINT,
      week_of_year TINYINT,
      day_of_month TINYINT,
      week_day TINYINT,
      the_date DATE,
      order_no INT,
      quarter_name CHAR(2),
      month_name VARCHAR(9),
      abbr_month_name VARCHAR(3),
      cn_month_name VARCHAR(9),
      week_name VARCHAR(9),
      abbr_week_name VARCHAR(3),
      cn_week_name VARCHAR(9),
      PRIMARY KEY (id)
    ) ENGINE=MyISAM COMMENT='日期维度';
    " | exec_sql

    range_date 20150525 20171231 | awk '{
        id=$1
        year=substr(id, 0, 4)
        month=int(substr(id, 5, 2))
        day=int(substr(id, 7, 2))
        the_time=mktime(year" "month" "day" 00 00 00")

        # 季度
        if(month >= 1 && month <= 3){
            quarter=1
        }else if(month >= 4 && month <= 6){
            quarter=2
        }else if(month >= 7 && month <= 9){
            quarter=3
        }else if(month >= 10 && month <= 12){
            quarter=4
        }

        # 半年
        if(quarter == 1 || quarter == 2){
            half_year=1
        }else if(quarter == 3 || quarter == 4){
            half_year=2
        }

        # 一年中的第几个星期(星期一作为一个星期的开始)
        week_of_year=strftime("%V", the_time)
        # 星期几(星期天是0)
        week_day=strftime("%w", the_time)
        if(week_day == 0){
            week_day=6
        }else{
            week_day--
        }

        # 排序用字段
        order_no=20890520 - id

        # 月名的完整写法(October)
        month_name=strftime("%B",the_time)
        # 月名的缩写(Oct)
        abbr_month_name=strftime("%b",the_time)

        # 星期几的完整写法(Sunday)
        week_name=strftime("%A",the_time)
        # 星期几的缩写(Sun)
        abbr_week_name=strftime("%a",the_time)

        # 简体中文月份
        cn_month[1]="一月"
        cn_month[2]="二月"
        cn_month[3]="三月"
        cn_month[4]="四月"
        cn_month[5]="五月"
        cn_month[6]="六月"
        cn_month[7]="七月"
        cn_month[8]="八月"
        cn_month[9]="九月"
        cn_month[10]="十月"
        cn_month[11]="十一月"
        cn_month[12]="十二月"

        # 简体中文周几
        cn_week[0]="周一"
        cn_week[1]="周二"
        cn_week[2]="周三"
        cn_week[3]="周四"
        cn_week[4]="周五"
        cn_week[5]="周六"
        cn_week[6]="周日"

        print "INSERT INTO dim_date (id, the_year, half_year, the_quarter, month_of_year, week_of_year, day_of_month, week_day, the_date, order_no, quarter_name, month_name, abbr_month_name, cn_month_name, week_name, abbr_week_name, cn_week_name) VALUES"
        printf("(%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, \"Q%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\");\n",id, year, half_year, quarter,month, week_of_year, day, week_day, id, order_no, quarter, month_name, abbr_month_name, cn_month[month], week_name, abbr_week_name, cn_week[week_day])
    }' | exec_sql
}

# 群组维度
function dim_cohort()
{
    echo ${1:-3600} | awk 'BEGIN{OFS="\t"}{
        for(i=0;i<=$1;i++){
            year=int(i/365)
            month=int(i%365/30)
            if(month>11) month=11
            week=int(i/7)

            print i, i + 1, week + 1, year * 12 + month + 1, year * 4 + int(month / 3) + 1, year + 1
        }
    }' > /tmp/dim_cohort.tmp

    echo "DROP TABLE IF EXISTS dim_cohort;
    CREATE TABLE dim_cohort (
      id SMALLINT(6),
      day_num VARCHAR(4),
      week_num VARCHAR(2),
      month_num VARCHAR(2),
      quarter_num VARCHAR(2),
      year_num VARCHAR(2),
      PRIMARY KEY(id)
    ) ENGINE =MyISAM COMMENT = '群组';
    INSERT INTO dim_cohort (id, day_num, week_num, month_num, quarter_num, year_num) VALUES (-1, '未知', '未知', '未知', '未知', '未知');
    LOAD DATA LOCAL INFILE 'dim_cohort.tmp' INTO TABLE dim_cohort (id, day_num, week_num, month_num, quarter_num, year_num);
    " | exec_sql

    # 删除临时文件
    rm -f /tmp/dim_cohort.tmp

    # 次数区间 App启动次数
    echo "ALTER TABLE dim_cohort ADD COLUMN times_1 SMALLINT(6), ADD COLUMN times_1_desc VARCHAR(10);
    UPDATE dim_cohort
    SET times_1 = CASE
    WHEN id <= 0 THEN 0
    WHEN id > 0 AND id <= 2 THEN 1
    WHEN id > 2 AND id <= 5 THEN 2
    WHEN id > 5 AND id <= 9 THEN 3
    WHEN id > 9 AND id <= 19 THEN 4
    WHEN id > 19 AND id <= 49 THEN 5
    ELSE 6
    END,
    times_1_desc = CASE
    WHEN id <= 0 THEN '未知'
    WHEN id > 0 AND id <= 2 THEN '1-2次'
    WHEN id > 2 AND id <= 5 THEN '3-5次'
    WHEN id > 5 AND id <= 9 THEN '6-9次'
    WHEN id > 9 AND id <= 19 THEN '10-19次'
    WHEN id > 19 AND id <= 49 THEN '20-49次'
    ELSE '50次+'
    END;
    " | exec_sql

    # 次数区间 亮屏次数
    echo "ALTER TABLE dim_cohort ADD COLUMN times_2 SMALLINT(6) AFTER times_1, ADD COLUMN times_2_desc VARCHAR(10) AFTER times_2;
    UPDATE dim_cohort
    SET times_2 = CASE
    WHEN id <= 0 THEN 0
    WHEN id = 1 THEN 1
    WHEN id > 1 AND id <= 10 THEN 2
    ELSE 3
    END,
    times_2_desc = CASE
    WHEN id <= 0 THEN '未知'
    WHEN id = 1 THEN '1次'
    WHEN id > 1 AND id <= 10 THEN '2-10次'
    ELSE '10次以上'
    END;
    " | exec_sql

    # 次数区间 客户端异常时间次数
    echo "ALTER TABLE dim_cohort ADD COLUMN times_3 SMALLINT(6) AFTER times_2, ADD COLUMN times_3_desc VARCHAR(10) AFTER times_3;
    UPDATE dim_cohort
    SET times_3 = CASE
    WHEN id <= 0 THEN 0
    WHEN id = 1 THEN 1
    WHEN id = 2 THEN 2
    ELSE 3
    END,
    times_3_desc = CASE
    WHEN id <= 0 THEN '0次'
    WHEN id = 1 THEN '1次'
    WHEN id = 2 THEN '2次'
    ELSE '3次+'
    END;
    " | exec_sql

    # 个数区间 电量个数
    echo "ALTER TABLE dim_cohort ADD COLUMN count_1 SMALLINT(6), ADD COLUMN count_1_desc VARCHAR(10);
    UPDATE dim_cohort
    SET count_1 = CASE
    WHEN id <= 0 THEN 0
    WHEN id = 1 THEN 1
    WHEN id > 1 AND id <= 10 THEN 2
    ELSE 3
    END,
    count_1_desc = CASE
    WHEN id <= 0 THEN '未知'
    WHEN id = 1 THEN '1个'
    WHEN id > 1 AND id <= 10 THEN '2-10个'
    ELSE '10个以上'
    END;
    " | exec_sql

    # 时长区间 session时长
    echo "ALTER TABLE dim_cohort ADD COLUMN time_1 SMALLINT(6), ADD COLUMN time_1_desc VARCHAR(10);
    UPDATE dim_cohort
    SET time_1 = CASE
    WHEN id <= 0 THEN 0
    WHEN id > 0 AND id <= 3 THEN 1
    WHEN id > 3 AND id <= 10 THEN 2
    WHEN id > 10 AND id <= 30 THEN 3
    WHEN id > 30 AND id <= 60 THEN 4
    WHEN id > 60 AND id <= 180 THEN 5
    WHEN id > 180 AND id <= 600 THEN 6
    WHEN id > 600 AND id <= 1800 THEN 7
    ELSE 8
    END,
    time_1_desc = CASE
    WHEN id <= 0 THEN '未知'
    WHEN id > 0 AND id <= 3 THEN '1~3秒'
    WHEN id > 3 AND id <= 10 THEN '4-10秒'
    WHEN id > 10 AND id <= 30 THEN '11-30秒'
    WHEN id > 30 AND id <= 60 THEN '31-60秒'
    WHEN id > 60 AND id <= 180 THEN '1-3分'
    WHEN id > 180 AND id <= 600 THEN '3-10分'
    WHEN id > 600 AND id <= 1800 THEN '10-30分'
    ELSE '30分~'
    END;
    " | exec_sql

    # 时长区间 进程运行时长
    echo "ALTER TABLE dim_cohort ADD COLUMN time_2 SMALLINT(6) AFTER time_1, ADD COLUMN time_2_desc VARCHAR(10) AFTER time_2;
    UPDATE dim_cohort
    SET time_2 = CASE
    WHEN id <= 0 THEN 0
    WHEN id = 1 THEN 1
    WHEN id > 1 AND id <= 10 THEN 2
    WHEN id > 10 AND id <= 30 THEN 3
    WHEN id > 30 AND id <= 120 THEN 4
    ELSE 5
    END,
    time_2_desc = CASE
    WHEN id <= 0 THEN '未知'
    WHEN id = 1 THEN '~1分'
    WHEN id > 1 AND id <= 10 THEN '1~10分'
    WHEN id > 10 AND id <= 30 THEN '10~30分'
    WHEN id > 30 AND id <= 120 THEN '30~120分'
    ELSE '2小时~'
    END;
    " | exec_sql
}

# 运营商维度
function dim_isp()
{
    echo "DROP TABLE IF EXISTS dim_isp;
    CREATE TABLE dim_isp (
      id INT(11),
      isp_name VARCHAR(64),
      PRIMARY KEY (id)
    ) ENGINE = MyISAM COMMENT = '运营商';
    INSERT INTO dim_isp (id, isp_name) VALUES
    (0, '未知'),
    (46000, '中国移动'),
    (46001, '中国联通'),
    (46002, '中国移动'),
    (46003, '中国电信'),
    (46007, '中国移动'),
    (46099, '中国电信');
    " | exec_sql
}

# 产品维度
function dim_product()
{
    echo "DROP TABLE IF EXISTS dim_product;
    CREATE TABLE dim_product (
      id VARCHAR(50),
      name VARCHAR(10),
      PRIMARY KEY (id)
    ) ENGINE = MyISAM COMMENT = '产品';
    INSERT INTO dim_product (id, name) VALUES
    ('jz-yaya', '牙牙关注'),
    ('jz-ylxs', '一路心喜'),
    ('jz-yyxq', '牙牙星球');
    " | exec_sql
}

function main()
{
    set_db ${SDK_DW[@]}

    dims="$1"

    if [[ -n "$dims" ]]; then
        echo "$dims" | tr ',' '\n' | while read dim; do
            log_fn dim_$dim
        done
    else
        log_fn dim_date
        log_fn dim_cohort
        log_fn dim_isp
    fi
}
main "$@"