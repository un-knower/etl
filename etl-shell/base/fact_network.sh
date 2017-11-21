#!/bin/bash
#
# 基线服务-网络类型事实表
# base_dw.fact_network


source $ETL_HOME/common/db_util.sh


function update_data()
{
    if [[ -z "$is_first" || $is_first -eq 0 ]]; then
        time_filter="AND createtime >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY"
    fi

    # 获取最新网络类型信息
    echo "SELECT 
      uuid,
      IF(link_type IN ('WIFI', '2G', '3G', '4G'), link_type, NULL) link_type,
      CASE 
      WHEN link_type = 'WIFI' THEN 1 
      WHEN link_type = '2G' THEN 2 
      WHEN link_type = '3G' THEN 3 
      WHEN link_type = '4G' THEN 4 
      ELSE 0 
      END order_num,
      stat_date 
    FROM base_dw.fact_network 
    UNION ALL 
    SELECT 
      uuid,
      IF(linktype IN ('WIFI', '2G', '3G', '4G'), linktype, NULL) link_type,
      CASE 
      WHEN linktype = 'WIFI' THEN 1 
      WHEN linktype = '2G' THEN 2 
      WHEN linktype = '3G' THEN 3 
      WHEN linktype = '4G' THEN 4 
      ELSE 0 
      END order_num,
      DATE(MAX(createtime)) stat_date 
    FROM base_ods.visitlog 
    WHERE uuid > '' 
    AND createtime < STR_TO_DATE('$run_time','%Y%m%d%H%i%s') 
    $time_filter 
    GROUP BY uuid, link_type;
    " | exec_sql > $data_path/log_network.tmp

    # 排序
    sort $data_path/log_network.tmp -o $data_path/log_network.tmp

    awk -F '\t' 'BEGIN{OFS=FS}{
        if($1 == p_uuid){
            if($3 > order_num) link_type=$2
            if($4 > stat_date) stat_date=$4
        }else{
            if(p_uuid != "") print p_uuid,link_type,stat_date
            link_type=$2
            order_num=$3
            stat_date=$4
        }
        p_uuid=$1
    }END{
        print p_uuid,link_type,stat_date
    }' $data_path/log_network.tmp | sort > $data_path/log_network.txt

    # 获取设备其他信息
    echo "SELECT uuid, customer_id, install_date FROM base_dw.fact_device;
    " | exec_sql | sort > $data_path/fact_device.tmp

    # 关联得到设备其他信息
    join -a 1 -e NULL -t "$sep" -o 1.1 1.2 2.2 2.3 1.3 $data_path/fact_device.tmp $data_path/log_network.txt |
    mysql_data_conv > $data_path/fact_network.txt

    # 装载数据
    echo "TRUNCATE TABLE base_dw.fact_network;
    LOAD DATA LOCAL INFILE '$data_path/fact_network.txt' IGNORE INTO TABLE base_dw.fact_network;
    UPDATE base_dw.fact_network SET link_type = 'Unknown' WHERE link_type IS NULL;
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