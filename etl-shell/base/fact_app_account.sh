#!/bin/bash
#
# 基线服务-APP账号事实表
# base_dw.fact_app_account


source $ETL_HOME/common/db_util.sh


function update_data()
{
    # 获取最新APP账号信息
    echo "SELECT uuid, COUNT(DISTINCT acc_type), DATE(MAX(create_time)) FROM base_ods.log_app_account GROUP BY uuid;
    " | exec_sql | sort > $data_path/log_app_account.tmp

    # 获取设备其他信息
    echo "SELECT uuid, customer_id, install_date FROM base_dw.fact_device;
    " | exec_sql | sort > $data_path/fact_device.tmp

    # 关联得到设备其他信息
    join -a 1 -e NULL -t "$sep" -o 1.1 1.2 2.2 2.3 1.3 $data_path/fact_device.tmp $data_path/log_app_account.tmp |
    mysql_data_conv > $data_path/fact_app_account.txt

    # 装载数据
    echo "TRUNCATE TABLE base_dw.fact_app_account;
    LOAD DATA LOCAL INFILE '$data_path/fact_app_account.txt' IGNORE INTO TABLE base_dw.fact_app_account;
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