#!/bin/bash
#
# 渠道监控告警


source $ETL_HOME/common/db_util.sh


# 告警渠道等级
ALARM_CHANNEL_RANKS="'D', 'E'"


# 发送告警
function send_alarm()
{
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
    # 设置数据库连接
    set_db ${SDK_DW[@]}

    echo "SELECT
      channel_id,
      channel_rank,
      user_count,
      keep_pct_1,
      keep_pct_3,
      keep_pct_6,
      vpn_pct,
      phone_pct,
      network_pct,
      runtime_pct,
      unlock_pct,
      battery_pct,
      etime_pct,
      station_pct
    FROM fact_channel
    WHERE stat_date = $the_day
    AND channel_rank IN ($ALARM_CHANNEL_RANKS)
    ORDER BY channel_rank DESC;
    " | exec_sql | awk -F '\t' 'BEGIN{OFS=FS}{
        printf("<tr><td>%s</td>",$1)
        for(i=2;i<NF;i++){
            if(i < 4){
                printf("<td>%s</td>",$i)
            }else{
                if($i == 0){
                    printf("<td></td>")
                }else{
                    printf("<td>%s%%</td>",$i * 100)
                }
            }
        }
        if($NF == 0){
            printf("<td></td></tr>")
        }else{
            printf("<td>%s%%</td></tr>",$NF * 100)
        }
    }' > $log_path/mail_content.tmp

    # 插入表头
    if [[ -s $log_path/mail_content.tmp ]]; then
        sed -i '1 i <table border="1"><tr><th>渠道编号</th><th>渠道等级</th><th>新增用户</th><th>次日留存率</th><th>3日留存率</th><th>6日留存率</th> <th>VPN用户占比</th> <th>移动号收集率</th> <th>WIFI+未知用户占比</th> <th>进程1分钟占比</th> <th>亮屏未知+1次占比</th> <th>电量未知+1个占比</th> <th>时间异常2次以上占比</th> <th>基站未知+1个占比</th></tr>' $log_path/mail_content.tmp
        sed -i '1 i <style>td{text-align:center}</style>' $log_path/mail_content.tmp
        sed -i '$ a </table>' $log_path/mail_content.tmp
    fi

    # 发送告警
    send_alarm
}
execute "$@"