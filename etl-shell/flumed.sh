#!/bin/bash

# flume守护进程
# 用法:
: '
FLUME_HOME=/usr/local/apache-flume-1.6.0
*/5 * * * * $FLUME_HOME/bin/flumed.sh 11000 $FLUME_HOME/conf/focus/visitlog.agent >> $FLUME_HOME/logs/flumed.log 2>&1
*/5 * * * * $FLUME_HOME/bin/flumed.sh 11001 $FLUME_HOME/conf/focus/clicklog.agent >> $FLUME_HOME/logs/flumed.log 2>&1
*/5 * * * * $FLUME_HOME/bin/flumed.sh 11000 $FLUME_HOME/conf/focus/visitlog.collector >> $FLUME_HOME/logs/flumed.log 2>&1
*/5 * * * * $FLUME_HOME/bin/flumed.sh 11001 $FLUME_HOME/conf/focus/clicklog.collector >> $FLUME_HOME/logs/flumed.log 2>&1
'


source /etc/profile
source ~/.bash_profile


function main()
{
    cur_time=`date +'%F %T'`

    # 参数判断
    if [[ $# -lt 2 || -z "${1// /}" ]]; then
        echo "$cur_time ERROR [ Invalid arguments: $@, usage: sh $0 <flume port> <flume conf> ]"
        exit 1
    fi

    # flume监听端口
    flume_port=$1
    # flume配置文件路径
    flume_conf=$2
    if [[ ! -s $flume_conf ]]; then
        echo "$cur_time ERROR [ Flume configuration file does not exists: $flume_conf ]"
        exit 1
    fi

    ps_count=`netstat -ant | grep :$flume_port | grep -v grep | wc -l`
    if [[ $ps_count -eq 0 ]]; then
        # 判断进程是否正常退出
        ps -ef | grep "$flume_conf" | grep -Ev "$0|grep" | awk '{print $2}' | while read pid; do
            echo "$cur_time WARN [ Kill process: $pid ]"
            kill $pid || kill -9 $pid
        done

        # flume启动命令，flume配置文件里面必须有
        start_cmd=`sed -n '/nohup flume-ng/p' $flume_conf | sed 's/.*\(nohup flume-ng\)/\1/'`
        if [[ -z "$start_cmd" ]]; then
            echo "$cur_time ERROR [ Can not find flume start command in flume configuration file: $flume_conf ]"
            exit 1
        fi

        echo "$cur_time INFO [ Start flume: $start_cmd ]"
        eval $start_cmd
    fi
}
main "$@"