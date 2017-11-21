#!/bin/bash
#
# 设备
# sdk_dw.fact_device
# 可用任务周期：天


source $ETL_HOME/common/db_util.sh


# 取数周期（最近一周）
INTERVAL_DAYS=7

# 客户端服务器时差异常临界值（分）
E_TIME_DIFF=30


function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    # 设置数据库连接
    debug "Set database connection: ${SDK_DW[@]}"
    set_db ${SDK_DW[@]}

    # 获取客户端信息
    log_task $LOG_LEVEL_INFO "Get client information from table: fact_client"
    echo "SELECT
      uuid,
      customer_id,
      DATE_FORMAT(create_time, '%Y%m%d')
    FROM fact_client
    WHERE app_key = 'jz-yaya';
    " | exec_sql | sort > $data_path/client.tmp

    # 进程时长、开屏、电量、客户端服务器时差
    log_task $LOG_LEVEL_INFO "Get process runtime, unlock, battery and so on"
    echo "SELECT
      uuid,
      MAX(boottime),
      MAX(screen_on),
      COUNT(DISTINCT battery),
      SUM(IF(ABS(time_diff / 60) > $E_TIME_DIFF, 1, 0))
    FROM fact_active
    WHERE time_diff IS NOT NULL
    AND active_date >= CURDATE() - INTERVAL $INTERVAL_DAYS DAY
    GROUP BY uuid;
    " | exec_sql | sort > $data_path/active.tmp
    debug "Join data: $data_path/client.tmp $data_path/active.tmp"
    join -t "$sep" $data_path/client.tmp $data_path/active.tmp | sort > $data_path/client_active.tmp

    # 基站
    log_task $LOG_LEVEL_INFO "Get base station"
    echo "SELECT
      uuid,
      COUNT(DISTINCT stationcode)
    FROM sdk_ods.ods_device_station
    WHERE uuid > ''
    AND stationcode > 0
    AND createtime >= CURDATE() - INTERVAL $INTERVAL_DAYS DAY
    GROUP BY uuid;
    " | exec_sql | sort > $data_path/station.tmp
    debug "Join data: $data_path/client_active.tmp $data_path/station.tmp"
    join -a 1 -e 0 -t "$sep" $data_path/client_active.tmp $data_path/station.tmp | sort > $data_path/client_station.tmp

    # 第三方应用账号
    log_task $LOG_LEVEL_INFO "Get app account"
    echo "SELECT
      uuid,
      COUNT(DISTINCT app_pkgname)
    FROM sdk_ods.ods_device_account
    WHERE uuid > ''
    AND createtime >= CURDATE() - INTERVAL $INTERVAL_DAYS DAY
    GROUP BY uuid;
    " | exec_sql | sort > $data_path/account.tmp
    debug "Join data: $data_path/client_station.tmp $data_path/account.tmp"
    join -a 1 -e 0 -t "$sep" $data_path/client_station.tmp $data_path/account.tmp | sort > $data_path/client_account.tmp

    # App安装卸载
    log_task $LOG_LEVEL_INFO "Get install and uninstall"
    echo "SELECT
      uuid,
      COUNT(DISTINCT app_pkgname)
    FROM sdk_ods.ods_device_addapps
    WHERE uuid > ''
    AND createtime >= CURDATE() - INTERVAL $INTERVAL_DAYS DAY
    GROUP BY uuid;
    " | exec_sql | sort > $data_path/app_install.tmp
    debug "Join data: $data_path/client_account.tmp $data_path/app_install.tmp"
    join -a 1 -e 0 -t "$sep" $data_path/client_account.tmp $data_path/app_install.tmp | sort > $data_path/client_install.tmp

    # App使用
    log_task $LOG_LEVEL_INFO "Get app using"
    echo "SELECT
      uuid,
      COUNT(DISTINCT app_pkgname)
    FROM sdk_ods.ods_device_useapps
    WHERE uuid > ''
    AND createtime >= CURDATE() - INTERVAL $INTERVAL_DAYS DAY
    GROUP BY uuid;
    " | exec_sql | sort > $data_path/app_use.tmp
    debug "Join data: $data_path/client_install.tmp $data_path/app_use.tmp"
    join -a 1 -e 0 -t "$sep" $data_path/client_install.tmp $data_path/app_use.tmp | mysql_data_conv > $data_path/fact_device.txt

    # 装载数据
    echo "LOCK TABLES fact_device WRITE;
    TRUNCATE TABLE fact_device;
    LOAD DATA LOCAL INFILE '$data_path/fact_device.txt' INTO TABLE fact_device (uuid,customer_id,create_date,runtime,unlock_cnt,battery_cnt,etime_cnt,station_cnt,account_cnt,install_cnt,appuse_cnt);
    UNLOCK TABLES;
    " | exec_sql
}
execute "$@"