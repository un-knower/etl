#!/bin/bash
#
# 基线服务-设备其他信息事实表
# base_dw.fact_other_info


source $ETL_HOME/common/db_util.sh


function update_data()
{
    if [[ -z "$is_first" || $is_first -eq 0 ]]; then
        time_filter="AND create_time >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY"
    fi

    # 获取最新设备其他信息
    echo "SELECT uuid, MAX(used_vpn), MAX(has_phone) 
    FROM 
    ( SELECT uuid, used_vpn, has_phone FROM base_dw.fact_other_info 
        UNION ALL 
      SELECT uuid, MAX(apn), IF(MAX(phone_num) > '', 1, 0) 
      FROM base_ods.log_other_info 
      WHERE uuid > '' 
      AND create_time < STR_TO_DATE('$run_time','%Y%m%d%H%i%s') $time_filter 
      GROUP BY uuid 
    ) t 
    GROUP BY uuid;
    " | exec_sql > $data_path/log_other_info.tmp

    # 获取设备其他信息
    echo "SELECT uuid, customer_id, install_date, isp_code FROM base_dw.fact_device;
    " | exec_sql | sort > $data_path/fact_device.tmp

    # 关联得到设备其他信息
    join -a 1 -e NULL -t "$sep" -o 1.1 1.2 1.3 1.4 2.2 2.3 $data_path/fact_device.tmp $data_path/log_other_info.tmp |
    mysql_data_conv > $data_path/fact_other_info.txt

    # 装载数据
    echo "TRUNCATE TABLE base_dw.fact_other_info;
    LOAD DATA LOCAL INFILE '$data_path/fact_other_info.txt' IGNORE INTO TABLE base_dw.fact_other_info (uuid, customer_id, install_date, isp_code, @used_vpn, @has_phone) 
    SET used_vpn = IFNULL(@used_vpn, -1), has_phone = IFNULL(@has_phone, -1);
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