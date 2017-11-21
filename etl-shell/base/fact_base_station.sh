#!/bin/bash
#
# 基线服务-基站事实表
# base_dw.fact_base_station


source $ETL_HOME/common/db_util.sh


function update_data()
{
    # 获取最新基站信息
    info "Get base station log"
    echo "SELECT uuid, station_code, create_time FROM base_ods.log_base_station;" | exec_sql > $data_path/log_base_station.tmp
    sort -t $'\t' -k 1 -k 3 $data_path/log_base_station.tmp -o $data_path/log_base_station.tmp
    awk -F '\t' 'BEGIN{OFS=FS}{
        if(uuid == $1){
            code[$2]=1
            create_time=$3
        }else{
            if(uuid > "") print uuid,length(code),create_time
            uuid=$1
            delete code
            code[$2]=1
            create_time=$3
        }
    }END{
        print uuid,length(code),create_time
    }' $data_path/log_base_station.tmp > $data_path/base_station.tmp
    sort $data_path/base_station.tmp -o $data_path/base_station.tmp

    # 获取设备其他信息
    info "Get device information"
    echo "SELECT uuid, customer_id, install_date FROM base_dw.fact_device;
    " | exec_sql | sort > $data_path/fact_device.tmp

    # 关联得到设备其他信息
    info "Join data"
    join -a 1 -e NULL -t "$sep" -o 1.1 1.2 2.2 2.3 1.3 $data_path/fact_device.tmp $data_path/base_station.tmp |
    mysql_data_conv > $data_path/fact_base_station.txt

    # 装载数据
    info "Load data into table: fact_base_station"
    echo "TRUNCATE TABLE base_dw.fact_base_station;
    LOAD DATA LOCAL INFILE '$data_path/fact_base_station.txt' IGNORE INTO TABLE base_dw.fact_base_station;
    " | exec_sql
}

function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    set_db ${BASE_DW[@]}

    update_data
}
execute "$@"