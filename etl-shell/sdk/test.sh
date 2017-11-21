#!/bin/bash

# 活跃
function fact_active()
{
    # 主动联网
    echo "SELECT
      CONCAT_WS('|', uuid, app_key, active_date),
      active_hours,
      time_diff,
      boottime,
      version,
      sdcard_size,
      rom_size,
      screen_on,
      battery,
      log_type,
      city,
      region,
      country,
      client_ip,
      visit_times,
      start_times,
      customer_id,
      init_version,
      create_date,
      date_diff
    FROM fact_active
    WHERE log_type = 1;
    " | mysql -uroot sdk_dw -s -N --local-infile | sort > active-1.tmp

    echo "step 1 done"

    # 使用时长
    echo "SELECT
      CONCAT_WS('|', uuid, app_key, create_date),
      SUM(duration)
    FROM fact_session GROUP BY 1;
    " | mysql -uroot sdk_dw -s -N --local-infile | sort > session-1.tmp

    echo "step 2 done"

    # 关联
    join -t "$sep" -a 1 active-1.tmp session-1.tmp | awk -F '\t' 'BEGIN{OFS=FS}{
        if(NF == 20){
            duration=0
        }else{
            duration=$21
        }
        split($1,arr,"|")
        print arr[1],arr[2],arr[3],$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,duration,$17,$18,$19,$20
    }' > active-1.txt

    echo "step 3 done"

    # 被动联网
    echo "SELECT
      uuid,
      app_key,
      active_date,
      active_hours,
      time_diff,
      boottime,
      version,
      sdcard_size,
      rom_size,
      screen_on,
      battery,
      log_type,
      city,
      region,
      country,
      client_ip,
      visit_times,
      start_times,
      0,
      customer_id,
      init_version,
      create_date,
      date_diff
    FROM fact_active
    WHERE log_type = 2;
    " | mysql -uroot sdk_dw -s -N --local-infile > active-2.txt

    echo "step 4 done"

    # 装载数据
    echo "RENAME TABLE fact_active TO fact_active_$(date +'%Y%m%d');
    CREATE TABLE fact_active LIKE fact_active_$(date +'%Y%m%d');
    ALTER TABLE fact_active ADD COLUMN duration BIGINT COMMENT '时长（秒）' AFTER start_times;
    LOAD DATA LOCAL INFILE 'active-1.txt' INTO TABLE fact_active;
    LOAD DATA LOCAL INFILE 'active-2.txt' INTO TABLE fact_active;
    " | mysql -uroot sdk_dw -s -N --local-infile

    echo "step 5 done"
}

# 小时活跃
function fact_hour_active()
{
    echo "SELECT active_hours FROM fact_active WHERE active_date = $prev_day AND log_type = 1;" | mysql -uroot sdk_dw -s -N --local-infile | awk 'BEGIN{OFS="\t"}{
        split($1,arr,",")
        for(i in arr){
            count[arr[i]]++
        }
    }END{
        for(k in count){
            print '$prev_day',k,count[k]
        }
    }' > hour_active.txt

    echo "CREATE TABLE IF NOT EXISTS fact_hour_active (
        active_date int,
        active_hour int,
        user_count int,
        PRIMARY KEY (active_date, active_hour)
    );

    LOAD DATA LOCAL INFILE 'hour_active.txt' INTO TABLE fact_hour_active;
    " | mysql -uroot sdk_dw -s -N --local-infile

    # 如果没有数据补0
    seq 0 23 | awk '{
        printf("INSERT IGNORE INTO fact_hour_active VALUES (%s, %s, %s);\n",'$prev_day',$1,0)
    }' | mysql -uroot sdk_dw -s -N --local-infile
}

function backup()
{
    dbs=(base_dw base_ods focus_dw focus_ods schedule)
    for db in ${dbs[@]}; do
        mysqldump -uroot --opt -R $db | gzip > ${db}.gz
        $SHELL_HOME/common/expect/autoscp.exp "*P=ZUs1$)NVQke]O" ${db}.gz mysqldba@10.10.10.172:/home/mysqldba/
    done
}

function main()
{
    set -e
    export LC_ALL=C
    sep=`echo -e "\t"`

    # 活跃
    fact_active

    range_date 20160801 20160901 | while read prev_day; do
        fact_hour_active
    done
}
main "$@"