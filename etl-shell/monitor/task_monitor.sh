#!/bin/bash
#
# ETL任务监控告警


source $ETL_HOME/common/db_util.sh


# 任务运行状态
function task_state()
{
    echo "SELECT 
      DATE(run_time),
      CASE task_state 
      WHEN 0 THEN '等待' 
      WHEN 1 THEN '就绪' 
      WHEN 2 THEN '已经分配' 
      WHEN 3 THEN '正在运行'
      WHEN 6 THEN '运行成功' 
      WHEN 9 THEN '运行失败' 
      ELSE '未知' 
      END,
      COUNT(1) 
    FROM t_task_pool 
    WHERE run_time >= CURDATE() - INTERVAL 2 DAY 
    GROUP BY DATE(run_time), task_state;
    " | exec_sql | awk -F '\t' 'BEGIN{
        print "<table border=1><tr><th>运行日期</th><th>任务状态</th><th>任务数</th></tr>"
    }{
        count[$1]++
        if(p_run_date == $1){
            last=last"<tr><td>"$2"</td><td>"$3"</td></tr>"
        }else{
            if(p_run_date != ""){
                gsub("row_count",count[p_run_date],first)
                print first""last
            }
            first="<tr><td rowspan=\"row_count\">"$1"</td><td>"$2"</td><td>"$3"</td></tr>"
            last=""
        }
        p_run_date=$1
    } END {
        gsub("row_count",count[p_run_date],first)
        print first""last
        print "</table>"
    }'
}

# 失败任务
function failed_task()
{
    echo "SELECT 
      a.task_id '任务ID',
      a.run_time '运行时间',
      c.task_group '任务组',
      c.name '任务名称',
      c.create_user '创建者',
      c.task_cycle '任务周期',
      a.tried_times '尝试次数',
      b.content '消息' 
    FROM t_task_pool a 
    INNER JOIN t_task_log b 
    INNER JOIN t_task c 
    ON a.task_id = b.task_id 
    AND a.task_id = c.id 
    AND a.run_time = b.run_time 
    AND a.task_state = 9 
    AND b.level = 3 
    AND a.start_time >= NOW() - INTERVAL 25 HOUR 
    GROUP BY a.task_id, a.run_time;
    " | exec_sql "" "-H --column-names"
}

# 今日任务耗时Top 10
function time_consume()
{
    echo "SELECT 
      a.task_id '任务ID',
      a.run_time '运行时间',
      b.task_group '任务组',
      b.name '任务名称',
      b.create_user '创建者',
      b.task_cycle '任务周期',
      ROUND(TIMESTAMPDIFF(SECOND, a.start_time, a.end_time) / 60, 2) '任务耗时（分）' 
    FROM t_task_pool a 
    INNER JOIN t_task b 
    ON a.task_id = b.id 
    AND b.task_cycle IN ('day', 'week', 'month', 'hour') 
    AND a.start_time >= NOW() - INTERVAL 25 HOUR 
    GROUP BY a.task_id, a.run_time 
    ORDER BY 7 DESC 
    LIMIT 10;
    " | exec_sql "" "-H --column-names"
}

# 生成邮件
function create_mail()
{
    # 任务运行状态
    echo "<H2>任务运行状态</H2>" > $log_path/mail_content.tmp
    task_state >> $log_path/mail_content.tmp

    # 失败任务
    failed_task > $log_path/failed_task.tmp
    if [[ -s $log_path/failed_task.tmp ]]; then
        echo "<H2>失败任务</H2>" >> $log_path/mail_content.tmp
        awk '{printf("%s<BR/>",$0)}' $log_path/failed_task.tmp >> $log_path/mail_content.tmp
    fi

    # 今日任务耗时Top 10
    echo "<H2>今日任务耗时Top 10</H2>" >> $log_path/mail_content.tmp
    time_consume >> $log_path/mail_content.tmp
}

# 发送告警
function send_alarm()
{
    # 生成邮件
    create_mail

    # 发送电子邮件
    if [[ -n "$sub_emails" ]]; then
        mail_subject="ETL任务监控告警"
        mail_content=`cat $log_path/mail_content.tmp`
        info "Send email notification to: $sub_emails"
        echo "$mail_content" | $SHELL_HOME/common/mail_sender.py "$sub_emails" "$mail_subject"
    else
        warn "There is no email subscribers"
    fi
}

function execute()
{
    set_db ${SCHEDULE[@]}

    # 发送告警
    send_alarm
}
execute "$@"