#!/bin/bash
#
# 解析sdk_ods.ods_device_visitlog获取设备信息
# sdk_dw.dim_device
# 可用任务周期：时/天/周/月


source $ETL_HOME/common/db_util.sh


function execute()
{
    export LC_ALL=C
    sep=`echo -e "\t"`

    # 设置数据库连接
    debug "Set database connection: ${BASE_ODS[@]}"
    set_db ${BASE_ODS[@]}

    # 获取旧版SDK Android ID
    info "Get android id from old sdk, source table: device"
    echo "SELECT aid, 1 FROM device WHERE aid > '' GROUP BY aid;" | exec_sql | sort > $data_path/android_id.tmp

    # 第一次取全量数据，之后取上一个周期数据
    if [[ $is_first -ne 1 ]]; then
        time_filter="AND createtime >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY"
        debug "Got time filter: $time_filter"

        # 任务重做
        time_filter1="AND update_time >= STR_TO_DATE('$run_time', '%Y%m%d%H%i%s') - INTERVAL 1 DAY AND update_time < STR_TO_DATE('$run_time', '%Y%m%d%H%i%s')"
        debug "Got time filter: $time_filter1"
    fi

    # 设置数据库连接
    debug "Set database connection: ${SDK_DW[@]}"
    set_db ${SDK_DW[@]}

    # 任务重做
    if [[ $redo_flag -eq 1 ]]; then
        log_task $LOG_LEVEL_INFO "Redo task, delete history data"
        echo "DELETE FROM dim_device WHERE 1 = 1 $time_filter1;" | exec_sql
    fi

    # 获取最新用户访问日志
    log_task $LOG_LEVEL_INFO "Get the latest user access log from table: ods_device_visitlog"
    echo "SELECT
      uuid,
      device_id,
      customer_id,
      network,
      platform,
      have_vpn,
      imsi,
      wifi_mac,
      imei,
      android_id,
      baseband,
      language,
      resolution,
      model_name,
      cpu,
      device_name,
      os_version,
      cameras,
      sdcard_size,
      rom_size,
      phone_no,
      city,
      region,
      country,
      uuid_type,
      isp_code,
      create_time,
      update_time
    FROM
    (
      SELECT
        uuid,
        device_id,
        customer_id,
        CASE
        WHEN network = 'WIFI' THEN '1WIFI'
        WHEN network = 'unknown' THEN '0unknown'
        ELSE network
        END network,
        platform,
        have_vpn,
        imsi,
        wifi_mac,
        imei,
        android_id,
        baseband,
        language,
        resolution,
        model_name,
        cpu,
        device_name,
        os_version,
        cameras,
        sdcard_size,
        rom_size,
        IF(phone_no = 'null', NULL, phone_no) phone_no,
        city,
        region,
        country,
        uuid_type,
        isp_code,
        create_time,
        update_time
      FROM dim_device
      UNION ALL
      SELECT
        uuid,
        deviceid,
        customid,
        CASE
        WHEN usednetwork LIKE '%4G%' THEN '4G'
        WHEN usednetwork LIKE '%3G%' THEN '3G'
        WHEN usednetwork LIKE '%2G%' THEN '2G'
        WHEN usednetwork LIKE '%WIFI%' THEN '1WIFI'
        ELSE '0unknown'
        END,
        IF(platform > '', platform, 'unknown'),
        IF(havaapn = 1, 1, 0),
        IF(imsi > '', imsi, NULL),
        IF(wifimac > '', wifimac, NULL),
        IF(imei > '', imei, NULL),
        IF(androidid > '', androidid, NULL),
        IF(baseband > '', baseband, NULL),
        IF(language > '', language, 'unknown'),
        IF(resolution > '', resolution, 'unknown'),
        IF(modulename > '', modulename, 'unknown'),
        IF(cpu > '', cpu, 'unknown'),
        IF(devicename > '', devicename, 'unknown'),
        IF(os_version > '', os_version, 'unknown'),
        IF(cameras > '', cameras, NULL),
        exttotalsize,
        romtotalsize,
        IF(phoneno > '', IF(phoneno = 'null', NULL, phoneno), NULL),
        IF(city > '', city, 'unknown'),
        IF(region > '', region, 'unknown'),
        IF(country > '', country, 'unknown'),
        uidtype,
        IF(imsi > '', LEFT(imsi, 5), 0),
        createtime,
        createtime
      FROM sdk_ods.ods_device_visitlog
      WHERE uuid > '' AND createtime > '2013-01-14'
      AND createtime < STR_TO_DATE('$run_time','%Y%m%d%H%i%s') $time_filter
    ) t
    ORDER BY uuid ASC, update_time ASC;
    " | exec_sql > $data_path/device.tmp

    # 解析用户访问日志得到全量设备信息
    log_task $LOG_LEVEL_INFO "Parse user access log to get full device information"
    awk -F '\t' 'BEGIN{
        OFS=FS
        IGNORECASE=1
    }{
        if($1 == uuid){
            device_id=$2
            if($4 > network) network=$4
            if(platform == "unknown") platform=$5
            if($6 > have_vpn) have_vpn=$6
            if(imsi == "NULL") imsi=$7
            if(wifi_mac == "NULL") wifi_mac=$8
            if(imei == "NULL") imei=$9
            if(android_id == "NULL") android_id=$10
            if(baseband == "NULL") baseband=$11
            if(language == "unknown") language=$12
            if(resolution == "unknown") resolution=$13
            if(model_name == "unknown") model_name=$14
            if(cpu == "unknown") cpu=$15
            if(device_name == "unknown") device_name=$16
            if($17 != "unknown") os_version=$17
            if(cameras == "NULL") cameras=$18
            if(sdcard_size == "NULL") sdcard_size=$19
            if(rom_size == "NULL") rom_size=$20
            if(phone_no == "NULL") phone_no=$21
            if(city ~ /^unknown$/) city=$22
            if(region ~ /^unknown$/) region=$23
            if(country ~ /^unknown$/) country=$24
            if(uuid_type == "NULL") uuid_type=$25
            if(isp_code == 0) isp_code=$26
            update_time=$28
        }else{
            if(uuid != ""){
                sub(/^[01]/,"",network)
                print uuid,device_id,customer_id,network,platform,have_vpn,imsi,wifi_mac,imei,android_id,baseband,language,resolution,model_name,cpu,device_name,os_version,cameras,sdcard_size,rom_size,phone_no,city,region,country,uuid_type,isp_code,create_time,update_time
            }
            uuid=$1
            device_id=$2
            customer_id=$3
            network=$4
            platform=$5
            have_vpn=$6
            imsi=$7
            wifi_mac=$8
            imei=$9
            android_id=$10
            baseband=$11
            language=$12
            resolution=$13
            model_name=$14
            cpu=$15
            device_name=$16
            os_version=$17
            cameras=$18
            sdcard_size=$19
            rom_size=$20
            phone_no=$21
            if($22 ~ /^unknown$/){
                city="unknown"
            }else{
                city=$22
            }
            if($23 ~ /^unknown$/){
                region="unknown"
            }else{
                region=$23
            }
            if($24 ~ /^unknown$/){
                country="unknown"
            }else{
                country=$24
            }
            uuid_type=$25
            if($26 ~ /^(46000|46001|46002|46003|46007|46099)$/){
                isp_code=$26
            }else{
                isp_code=0
            }
            create_time=$27
            update_time=$28
        }
    } END {
        sub(/^[01]/,"",network)
        print uuid,device_id,customer_id,network,platform,have_vpn,imsi,wifi_mac,imei,android_id,baseband,language,resolution,model_name,cpu,device_name,os_version,cameras,sdcard_size,rom_size,phone_no,city,region,country,uuid_type,isp_code,create_time,update_time
    }' $data_path/device.tmp | sort -t $'\t' -k 10 > $data_path/dim_device.tmp

    # 判断Android ID是否存在于旧版SDK
    info "Join data to get if android id exists in old sdk"
    join -t "$sep" -1 10 -2 1 -a 1 -e 0 -o 1.1 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 1.18 1.19 1.20 1.21 1.22 1.23 1.24 1.25 1.26 1.27 1.28 2.2 $data_path/dim_device.tmp $data_path/android_id.tmp |
    sort > $data_path/dim_device-1.tmp

    # 判断设备是否已注册
    info "Get registered device from: user_register"
    echo "SELECT uuid, 1 FROM sdk_ods.user_register;" | exec_sql | sort > $data_path/user_register.tmp
    info "Join data to get if device has registered"
    join -t "$sep" -a 1 -e 0 -o 1.1 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 1.18 1.19 1.20 1.21 1.22 1.23 1.24 1.25 1.26 1.27 1.28 1.29 2.2 $data_path/dim_device-1.tmp $data_path/user_register.tmp |
    mysql_data_conv > $data_path/dim_device.txt

    # 导入数据
    log_task $LOG_LEVEL_INFO "Load data to table: dim_device"
    echo "LOCK TABLES dim_device WRITE;
    TRUNCATE TABLE dim_device;
    LOAD DATA LOCAL INFILE '$data_path/dim_device.txt' INTO TABLE dim_device
    (uuid,device_id,customer_id,network,platform,have_vpn,imsi,wifi_mac,imei,android_id,baseband,language,resolution,model_name,cpu,device_name,os_version,cameras,sdcard_size,rom_size,phone_no,city,region,country,uuid_type,isp_code,create_time,update_time,is_exists,is_registered);
    UNLOCK TABLES;
    " | exec_sql
}
execute "$@"