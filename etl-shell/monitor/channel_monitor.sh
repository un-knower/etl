#!/bin/bash
#
# 渠道异常监控告警


source $ETL_HOME/common/db_util.sh


# 排除渠道
exclude_channels="'\${CHANNEL_VALUE}', 'J66666666'"

# 开始日期
begin_date="2015-12-01"

# 最低安装量
min_install_cnt=50

# 安装留存率
install_keep_pct_1=5          # 安装次日留存率
install_keep_pct_7=2          # 安装7天后留存率
install_keep_pct_14=1         # 安装14天后留存率

# WIFI占比
wifi_pct=95

# 移动号码占比
phone_pct=1

# VPN占比
vpn_pct=50

# 亮屏2次以上占比
unlock_pct=2

# 牙牙关注活跃用户环比
focus_active_qoq=50


# 初始化
function init()
{
    # 创建表，删除1个月前的历史数据
    echo "CREATE TABLE IF NOT EXISTS base_ods.channel_alarm(
      stat_date DATE,
      customer_id VARCHAR(50),
      msg VARCHAR(255),
      show_order TINYINT
    ) COMMENT '渠道告警信息';
    DELETE FROM base_ods.channel_alarm WHERE stat_date < '$the_day' - INTERVAL 1 MONTH;
    DELETE FROM base_ods.channel_alarm WHERE stat_date = '$the_day';
    " | exec_sql
}

# 安装留存率
function install_keep()
{
    # 次日留存率
    echo "SELECT customer_id,
    ROUND(
      COUNT(
        DISTINCT uuid,
        IF(day_diff = 1 AND visit_date < '$the_day', 1, NULL)
      ) / 
      COUNT(
        DISTINCT uuid,
        IF(day_diff = 0 AND install_date < '$the_day' - INTERVAL 1 DAY, 1, NULL)
      ) * 100, 2
    ) keep_pct,
    COUNT(
      DISTINCT uuid,
      IF(day_diff = 0 AND install_date < '$the_day', 1, NULL)
    ) new_cnt 
    FROM base_dw.fact_active 
    WHERE customer_id > '' 
    AND customer_id NOT IN ($exclude_channels) 
    AND install_date >= '$begin_date' 
    GROUP BY customer_id 
    HAVING keep_pct < $install_keep_pct_1 
    AND new_cnt > $min_install_cnt;
    " | exec_sql | awk -F '\t' '{
        printf("INSERT INTO base_ods.channel_alarm(stat_date, customer_id, msg, show_order) VALUES(\"%s\", \"%s\", \"安装次日留存率：%s%\", 1);\n",stat_date,$1,$2)
    }' stat_date=$the_day | exec_sql

    # 7天后留存率
    echo "SELECT customer_id,
    ROUND(
      COUNT(
        DISTINCT uuid,
        IF(day_diff >= 7 AND visit_date < '$the_day' - INTERVAL 6 DAY, 1, NULL)
        ) / 
      COUNT(
        DISTINCT uuid,
        IF(day_diff = 0 AND install_date < '$the_day' - INTERVAL 7 DAY, 1, NULL)
      ) * 100, 2
    ) keep_pct,
    COUNT(
      DISTINCT uuid,
      IF(day_diff = 0 AND install_date < '$the_day', 1, NULL)
    ) new_cnt 
    FROM base_dw.fact_active 
    WHERE customer_id > '' 
    AND customer_id NOT IN ($exclude_channels) 
    AND install_date >= '$begin_date' 
    GROUP BY customer_id 
    HAVING keep_pct < $install_keep_pct_7 
    AND new_cnt > $min_install_cnt;
    " | exec_sql | awk -F '\t' '{
        printf("INSERT INTO base_ods.channel_alarm(stat_date, customer_id, msg, show_order) VALUES(\"%s\", \"%s\", \"安装7天后留存率：%s%\", 2);\n",stat_date,$1,$2)
    }' stat_date=$the_day | exec_sql

    # 14天后留存率
    echo "SELECT customer_id,
    ROUND(
      COUNT(
        DISTINCT uuid,
        IF(day_diff >= 14 AND visit_date < '$the_day' - INTERVAL 13 DAY, 1, NULL)
      ) / 
      COUNT(
        DISTINCT uuid,
        IF(day_diff = 0 AND install_date < '$the_day' - INTERVAL 14 DAY, 1, NULL)
      ) * 100, 2
    ) keep_pct,
    COUNT(
      DISTINCT uuid,
      IF(day_diff = 0 AND install_date < '$the_day', 1, NULL)
    ) new_cnt 
    FROM base_dw.fact_active 
    WHERE customer_id > '' 
    AND customer_id NOT IN ($exclude_channels) 
    AND install_date >= '$begin_date' 
    GROUP BY customer_id 
    HAVING keep_pct < $install_keep_pct_14 
    AND new_cnt > $min_install_cnt;
    " | exec_sql | awk -F '\t' '{
        printf("INSERT INTO base_ods.channel_alarm(stat_date, customer_id, msg, show_order) VALUES(\"%s\", \"%s\", \"安装14天后留存率：%s%\", 3);\n",stat_date,$1,$2)
    }' stat_date=$the_day | exec_sql

    # 前天次日留存率
    echo "SELECT customer_id,
    ROUND(
      COUNT(
        DISTINCT uuid,
        IF(day_diff = 1, 1, NULL)
      ) / 
      COUNT(
        DISTINCT uuid,
        IF(day_diff = 0, 1, NULL)
      ) * 100, 2
    ) keep_pct,
    COUNT(
      DISTINCT uuid,
      IF(day_diff = 0, 1, NULL)
    ) new_cnt 
    FROM base_dw.fact_active 
    WHERE customer_id > '' 
    AND customer_id NOT IN ($exclude_channels) 
    AND install_date = '$the_day' - INTERVAL 2 DAY 
    GROUP BY customer_id 
    HAVING keep_pct < $install_keep_pct_1 
    AND new_cnt > $min_install_cnt;
    " | exec_sql | awk -F '\t' '{
        printf("INSERT INTO base_ods.channel_alarm(stat_date, customer_id, msg, show_order) VALUES(\"%s\", \"%s\", \"安装前天次日留存率：%s%\", 4);\n",stat_date,$1,$2)
    }' stat_date=$the_day | exec_sql
}

# WIFI占比
function wifi_pct()
{
    echo "SELECT customer_id,
    ROUND(SUM(IF(link_type = 'WIFI', 1, 0)) / COUNT(1) * 100, 2) wifi_pct,
    COUNT(1) new_cnt 
    FROM fact_network 
    WHERE customer_id > '' 
    AND customer_id NOT IN ($exclude_channels) 
    AND install_date >= '$begin_date' 
    GROUP BY customer_id 
    HAVING wifi_pct > $wifi_pct 
    AND new_cnt > $min_install_cnt;
    " | exec_sql | awk -F '\t' '{
        printf("INSERT INTO base_ods.channel_alarm(stat_date, customer_id, msg, show_order) VALUES(\"%s\", \"%s\", \"WIFI占比：%s%\", 5);\n",stat_date,$1,$2)
    }' stat_date=$the_day | exec_sql
}

# VPN使用率
function vpn_pct()
{
    echo "SELECT customer_id,
    ROUND(SUM(IF(used_vpn = 1, 1, 0)) / SUM(IF(used_vpn >= 0, 1, 0)) * 100, 2) vpn_pct,
    COUNT(1) new_cnt 
    FROM fact_other_info 
    WHERE customer_id > '' 
    AND customer_id NOT IN ($exclude_channels) 
    AND install_date >= '$begin_date' 
    GROUP BY customer_id 
    HAVING vpn_pct > $vpn_pct 
    AND new_cnt > $min_install_cnt;
    " | exec_sql | awk -F '\t' '{
        printf("INSERT INTO base_ods.channel_alarm(stat_date, customer_id, msg, show_order) VALUES(\"%s\", \"%s\", \"VPN使用率：%s%\", 6);\n",stat_date,$1,$2)
    }' stat_date=$the_day | exec_sql
}

# 移动号码收集率
function phone_pct()
{
    echo "SELECT customer_id,
    ROUND(SUM(IF(has_phone = 1, 1, 0)) / SUM(IF(isp_code IN (46000, 46002, 46007), 1, 0)) * 100, 2) phone_pct,
    COUNT(1) new_cnt 
    FROM fact_other_info 
    WHERE customer_id > '' 
    AND customer_id NOT IN ($exclude_channels) 
    AND install_date >= '$begin_date' 
    GROUP BY customer_id 
    HAVING phone_pct < $phone_pct 
    AND new_cnt > $min_install_cnt;
    " | exec_sql | awk -F '\t' '{
        printf("INSERT INTO base_ods.channel_alarm(stat_date, customer_id, msg, show_order) VALUES(\"%s\", \"%s\", \"移动号码收集率：%s%\", 7);\n",stat_date,$1,$2)
    }' stat_date=$the_day | exec_sql
}

# 亮屏2次以上占比
function unlock_pct()
{
    echo "SELECT customer_id,
    ROUND(SUM(IF(unlock_count >= 2, 1, 0)) / COUNT(1) * 100, 2) unlock_pct,
    COUNT(1) new_cnt 
    FROM fact_locked 
    WHERE customer_id > '' 
    AND customer_id NOT IN ($exclude_channels) 
    AND install_date >= '$begin_date' 
    GROUP BY customer_id 
    HAVING unlock_pct < $unlock_pct 
    AND new_cnt > $min_install_cnt;
    " | exec_sql | awk -F '\t' '{
        printf("INSERT INTO base_ods.channel_alarm(stat_date, customer_id, msg, show_order) VALUES(\"%s\", \"%s\", \"亮屏2次以上占比：%s%\", 8);\n",stat_date,$1,$2)
    }' stat_date=$the_day | exec_sql
}

# 牙牙关注活跃用户
function focus_active()
{
    # 活跃用户环比下降
    echo "SELECT customer_id, ROUND((pprev_day - prev_day) / pprev_day * 100, 2) qoq_pct 
    FROM (
      SELECT customer_id,
      COUNT(DISTINCT uuid, IF(visit_date > activate_date AND visit_date = '$the_day' - INTERVAL 2 DAY, 1, NULL)) pprev_day,
      COUNT(DISTINCT uuid, IF(visit_date > activate_date AND visit_date = '$the_day' - INTERVAL 1 DAY, 1, NULL)) prev_day
      FROM focus_dw.fact_active 
      WHERE customer_id > '' 
      AND customer_id NOT IN ($exclude_channels) 
      GROUP BY customer_id 
    ) t 
    GROUP BY customer_id 
    HAVING qoq_pct > $focus_active_qoq;
    " | exec_sql | awk -F '\t' '{
        printf("INSERT INTO base_ods.channel_alarm(stat_date, customer_id, msg, show_order) VALUES(\"%s\", \"%s\", \"牙牙关注昨日活跃用户环比下降：%s%\", 9);\n",stat_date,$1,$2)
    }' stat_date=$the_day | exec_sql
}

# 生成邮件
function create_mail()
{
    echo "SELECT
      b.cusname,
      b.chacode,
      GROUP_CONCAT(a.msg ORDER BY a.show_order SEPARATOR ' | ') 
    FROM base_ods.channel_alarm a 
    INNER JOIN base_ods.t_channel b 
    ON a.customer_id = b.chacode 
    AND b.alarm = 1 
    AND a.stat_date = '$the_day' 
    GROUP BY b.chacode 
    ORDER BY b.cusname, b.chacode;
    " | exec_sql | awk -F '\t' '{
        count[$1]++
        if(pcusname == $1){
            last=last"<tr><td>"$2"</td><td>"$3"</td></tr>"
        }else{
            if(pcusname != ""){
                gsub("row_count",count[pcusname],first)
                print first""last
            }
            first="<tr><td rowspan=\"row_count\">"$1"</td><td>"$2"</td><td>"$3"</td></tr>"
            last=""
        }
        pcusname=$1
    } END {
        gsub("row_count",count[pcusname],first)
        print first""last
    }' > $log_path/mail_content.tmp

    # 插入表头
    if [[ -s $log_path/mail_content.tmp ]]; then
        sed -i '1 i <table border="1"><tr><th>客户名称</th><th>渠道编号</th><th>异常指标</th></tr>' $log_path/mail_content.tmp
        sed -i '$ a </table>' $log_path/mail_content.tmp
    fi
}

# 发送告警
function send_alarm()
{
    # 生成邮件
    create_mail

    # 发送电子邮件
    if [[ -s $log_path/mail_content.tmp ]]; then
        if [[ -n "$sub_emails" ]]; then
            mail_subject="渠道异常告警"
            mail_content=`cat $log_path/mail_content.tmp`
            info "Send email notification to: $sub_emails"
            $SHELL_HOME/common/mail_sender.py "$sub_emails" "$mail_subject" "$mail_content"
        else
            warn "There is no email subscribers"
        fi
    else
        info "There is no abnormal channels"
    fi
}

function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    set_db ${BASE_DW[@]}

    # 初始化
    init

    # 安装留存率
    install_keep

    # WIFI占比
    wifi_pct

    # VPN占比
    vpn_pct

    # 移动号码占比
    phone_pct

    # 亮屏次数占比
    unlock_pct

    # 牙牙关注活跃用户
    focus_active

    # 发送告警
    send_alarm
}
execute "$@"