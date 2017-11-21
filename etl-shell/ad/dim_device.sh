#!/bin/bash
#
# 日志解析
# adv_visitlog -> dim_device
# 可选任务周期：时/天/周/月


source $ETL_HOME/common/db_util.sh


function execute()
{
    export LC_ALL=C

    # 设置数据库连接
    debug "Set database connection: ${AD_ODS[@]}"
    set_db ${AD_ODS[@]}

    # 第一次取全量数据，之后取上一个周期数据
    if [[ $is_first -ne 1 ]]; then
        time_filter="AND create_time >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY"
        debug "Got time filter: $time_filter"
    fi

    # 获取增量数据
    debug "Get incremental access log from table: adv_visitlog"
    echo "SELECT
      androidid,
      create_time,
      create_time update_time,
      custom_id,
      IF(brand > '', brand, NULL),
      city_id,
      IF(imei > '', imei, NULL),
      IF(imsi > '', imsi, NULL),
      IF(mac > '', mac, NULL),
      IF(model > '', model, NULL),
      CASE nettype WHEN 1 THEN 2 WHEN 2 THEN 1 else nettype END,
      rom,
      sdcard,
      IF(src > '', src, NULL),
      IF(sysver > '', sysver, NULL),
      IF(version > '', version, NULL),
      IF(version > '', version, NULL) init_version,
      path,
      root
    FROM adv_visitlog
    WHERE androidid > '' AND custom_id > ''
    AND create_time < STR_TO_DATE('$run_time','%Y%m%d%H%i%s') $time_filter;
    " | exec_sql > $data_path/visitlog.tmp

    # 设置数据库连接
    debug "Set database connection: ${AD_DW[@]}"
    set_db ${AD_DW[@]}

    # 获取当前设备信息
    debug "Get current device information from table: dim_device"
    echo "SELECT
      device_id,
      create_time,
      update_time,
      custom_id,
      brand,
      city_id,
      imei,
      imsi,
      mac,
      model,
      nettype,
      rom,
      sdcard,
      src,
      sysver,
      version,
      init_version,
      app_path,
      is_root
    FROM dim_device;
    " | exec_sql > $data_path/device.tmp

    # 合并设备信息
    debug "Merge device information"
    cat $data_path/device.tmp $data_path/visitlog.tmp | sort -t $'\t' -k 1 -k 3 | awk -F '\t' 'BEGIN{OFS=FS}{
        if($1 == device_id){
            update_time=$3
            if($5 != "NULL") brand=$5
            if($6 > 0) city_id=$6
            if($7 != "NULL") imei=$7
            if($8 != "NULL") imsi=$8
            if($9 != "NULL") mac=$9
            if($10 != "NULL") model=$10
            if($11 > nettype) nettype=$11
            if($12 > 0) rom=$12
            if($13 > 0) sdcard=$13
            if($14 != "NULL") src=$14
            if($15 != "NULL") sysver=$15
            if($16 != "NULL") version=$16
            if($18 > 0) app_path=$18
            if($19 > 0) is_root=$19
        }else{
            if(device_id != "") print device_id,custom_id,brand,city_id,imei,imsi,mac,model,nettype,rom,sdcard,src,sysver,version,init_version,app_path,is_root,create_time,update_time
            device_id=$1
            create_time=$2
            update_time=$3
            custom_id=$4
            brand=$5
            city_id=$6
            imei=$7
            imsi=$8
            mac=$9
            model=$10
            nettype=$11
            rom=$12
            sdcard=$13
            src=$14
            sysver=$15
            version=$16
            init_version=$17
            app_path=$18
            is_root=$19
        }
    }END{
        print device_id,custom_id,brand,city_id,imei,imsi,mac,model,nettype,rom,sdcard,src,sysver,version,init_version,app_path,is_root,create_time,update_time
    }' | mysql_data_conv > $data_path/dim_device.txt

    # 导入数据
    log_task $LOG_LEVEL_INFO "Load data to table: dim_device"
    echo "LOCK TABLES dim_device WRITE;
    TRUNCATE TABLE dim_device;
    LOAD DATA LOCAL INFILE '$data_path/dim_device.txt' INTO TABLE dim_device
    (device_id,custom_id,brand,city_id,imei,imsi,mac,model,nettype,rom,sdcard,src,sysver,version,init_version,app_path,is_root,create_time,update_time);
    UNLOCK TABLES;
    " | exec_sql
}
execute "$@"