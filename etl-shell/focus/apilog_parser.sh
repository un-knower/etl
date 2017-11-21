#!/bin/bash
#
# 牙牙关注api日志解析器


BASE_DIR=`pwd`
REL_DIR=`dirname $0`
cd $REL_DIR
DIR=`pwd`
cd - > /dev/null


source /etc/profile
source ~/.bash_profile
source $SHELL_HOME/common/include.sh
source $SHELL_HOME/common/date_util.sh


# 捕捉kill信号
trap 'warn "$0 is killed, pid: $$, script will exit soon";unset flag' term


# 日志文件目录
LOG_DIR=/var/flume/sink/data/collector/focus/apilog
# 文件检测时间间隔
FILE_CHECK_INTERVAL=30


function execute()
{
    ls -cr $LOG_DIR | egrep -v ".DONE$" | sed '$d' | while read file_name; do
        # 空文件判断
        if [[ ! -s $LOG_DIR/$file_name ]]; then
            rm -f $LOG_DIR/$file_name
            continue
        fi

        # 解析json数据
        cat $LOG_DIR/$file_name | while read line; do
            echo "$line" > line.log
            echo -e "line.log\n" | awk -f $SHELL_HOME/common/JSON.awk | awk 'BEGIN{
                OFS="\t"
                IGNORECASE=1
            }{
                # 获取参数名和参数值
                key=substr($1,3,length($1)-4)
                value=substr($2,2,length($2)-2)

                if(key ~ /^method$/){
                    method=value
                }else if(key ~ /^device$/){
                    device=value
                }else if(key ~ /^udid$|^uuid$/){
                    udid=value
                }else if(key ~ /^appCode$/){
                    appCode=value
                }else if(key ~ /^appVer$/){
                    appVer=value
                }else if(key ~ /^trackId$/){
                    trackId=value
                }else if(key ~ /^platform$/){
                    platform=value
                }else if(key ~ /^clnt$/){
                    clnt=value
                }else if(key ~ /^src$/){
                    src=value
                }else if(key ~ /^lat$/){
                    lat=value
                }else if(key ~ /^lng$/){
                    lng=value
                }else if(key ~ /^androidid$/){
                    androidid=value
                }else if(key ~ /^imei$/){
                    imei=value
                }else if(key ~ /^imsi$/){
                    imsi=value
                }else if(key ~ /^categoryId$/){
                    categoryId=value
                }else if(key ~ /^informationId$/){
                    informationId=value
                }
            } END {
                if(method == "jz.yaya.information.list"){
                    print device,udid,appCode,appVer,trackId,platform,clnt,src,lat,lng,androidid,imei,imsi,categoryId >> "/home/mysqldba/"method
                }else if(method == "jz.yaya.information.browse"){
                    print device,udid,appCode,appVer,trackId,platform,clnt,src,lat,lng,androidid,imei,imsi,informationId >> "/home/mysqldba/"method
                }else if(method == "jz.yaya.information.index"){
                    print device,udid,appCode,appVer,trackId,platform,clnt,src,lat,lng,androidid,imei,imsi >> "/home/mysqldba/"method
                }
            }'
        done

        # 重命名文件
        mv $LOG_DIR/$file_name $LOG_DIR/${file_name}.DONE
    done
}

function main()
{
    info "Current working directory: $BASE_DIR, invoke script: $0 $@"

    # 出错立即退出
    set -e

    flag="$1"
    if [[ "$flag" = "$CMD_STAY" ]]; then
        info "Script will execute periodically"

        shift
        while [[ "$flag" = "$CMD_STAY" ]]; do
            execute

            if [[ "$flag" != "$CMD_STAY" ]]; then
                break
            fi

            info "$0 sleep for $FILE_CHECK_INTERVAL"
            sleep $FILE_CHECK_INTERVAL
            info "$0 wake up"
        done
    else
        info "Script will execute one time and exit"

        execute
    fi
}
main "$@"