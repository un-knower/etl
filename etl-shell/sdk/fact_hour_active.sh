#!/bin/bash
#
# 分时段活跃
# sdk_dw.fact_hour_active
# 可用任务周期：天


source $ETL_HOME/common/db_util.sh


function execute()
{
    # 设置数据库连接
    debug "Set database connection: ${SDK_DW[@]}"
    set_db ${SDK_DW[@]}

    # 从日活跃表获取时段活跃数据
    log_task $LOG_LEVEL_INFO "Get hour active from table: fact_active"
    echo "SELECT customer_id, version, active_hours FROM fact_active WHERE active_date = $prev_day AND log_type = 1;" | exec_sql | awk 'BEGIN{OFS="\t"}{
        split($3,arr,",")
        for(i in arr){
            count[$1"|"$2"|"arr[i]]++
        }
        print $1,$2 >> "'$data_path'/other.tmp"
    }END{
        for(k in count){
            split(k,arr,"|")
            print arr[1],arr[2],'$prev_day',arr[3],count[k]
        }
    }' > $data_path/hour_active.txt

    # 任务重做
    if [[ $redo_flag -eq 1 ]]; then
        log_task $LOG_LEVEL_INFO "Redo task, delete history data"
        echo "DELETE FROM fact_hour_active WHERE active_date = $prev_day;" | exec_sql
    fi

    # 导入数据
    log_task $LOG_LEVEL_INFO "Load data to table: fact_hour_active"
    echo "LOAD DATA LOCAL INFILE '$data_path/hour_active.txt' INTO TABLE fact_hour_active;
    " | exec_sql

    # 如果没有数据补0
    sort -u $data_path/other.tmp | awk '{
        for(i=0;i<24;i++){
            printf("INSERT IGNORE INTO fact_hour_active (customer_id, version, active_date, active_hour, user_count) VALUES (\"%s\", \"%s\", %s, %s, %s);\n",$1,$2,'$prev_day',i,0)
        }
    }' | exec_sql
}
execute "$@"