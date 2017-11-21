#!/bin/bash
#
# 基线服务-亮屏事实表
# base_dw.fact_locked


source $ETL_HOME/common/db_util.sh


function update_data()
{
    # 获取最新亮屏信息
    echo "SELECT uuid, COUNT(DISTINCT unlock_cnt), DATE(MAX(create_time)) 
    FROM base_ods.log_locked 
    WHERE unlock_cnt > 0 
    GROUP BY uuid;
    " | exec_sql | sort > $data_path/log_locked.tmp

    # 获取设备其他信息
    echo "SELECT uuid, customer_id, install_date FROM base_dw.fact_device;
    " | exec_sql | sort > $data_path/fact_device.tmp

    # 关联得到设备其他信息
    join -a 1 -e NULL -t "$sep" -o 1.1 1.2 2.2 2.3 1.3 $data_path/fact_device.tmp $data_path/log_locked.tmp |
    mysql_data_conv > $data_path/fact_locked.txt

    # 装载数据
    echo "TRUNCATE TABLE base_dw.fact_locked;
    LOAD DATA LOCAL INFILE '$data_path/fact_locked.txt' IGNORE INTO TABLE base_dw.fact_locked;
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