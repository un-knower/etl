#!/bin/bash
#
# 基线服务-电量事实表
# base_dw.fact_power


source $ETL_HOME/common/db_util.sh


function update_data()
{
    # 获取最新电量信息
    info "Get power log"
    echo "SELECT uuid, low_power, create_time FROM base_ods.log_power WHERE low_power > 0 AND low_power < 101;" | exec_sql > $data_path/log_power.tmp
    sort -t $'\t' -k 1 -k 3 $data_path/log_power.tmp -o $data_path/log_power.tmp
    awk -F '\t' 'BEGIN{OFS=FS}{
        if(uuid == $1){
            power[$2]=1
            create_time=$3
        }else{
            if(uuid > "") print uuid,length(power),create_time
            uuid=$1
            delete power
            power[$2]=1
            create_time=$3
        }
    }END{
        print uuid,length(power),create_time
    }' $data_path/log_power.tmp > $data_path/power.tmp
    sort $data_path/power.tmp -o $data_path/power.tmp

    # 获取设备其他信息
    info "Get device information"
    echo "SELECT uuid, customer_id, install_date FROM base_dw.fact_device;
    " | exec_sql | sort > $data_path/fact_device.tmp

    # 关联得到设备其他信息
    info "Join data"
    join -a 1 -e NULL -t "$sep" -o 1.1 1.2 2.2 2.3 1.3 $data_path/fact_device.tmp $data_path/power.tmp |
    mysql_data_conv > $data_path/fact_power.txt

    # 装载数据
    info "Load data into table: fact_power"
    echo "TRUNCATE TABLE base_dw.fact_power;
    LOAD DATA LOCAL INFILE '$data_path/fact_power.txt' IGNORE INTO TABLE base_dw.fact_power;
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