#!/bin/bash
#
# 真假判断


BASE_DIR=`pwd`
REL_DIR=`dirname $0`
cd $REL_DIR
DIR=`pwd`
cd - > /dev/null


# 公共脚本目录
COMMON_DIR=.

source $COMMON_DIR/common.sh
source $COMMON_DIR/judge_config.sh


# 创建表
function create_tables()
{
    echo "CREATE TABLE IF NOT EXISTS dc_ods.integral_rule (
      id int COMMENT '主键',
      dim_type varchar(32) COMMENT '维度类型',
      rule_code varchar(64) COMMENT '规则编码',
      rule_desc varchar(255) COMMENT '规则描述',
      time_range varchar(32) COMMENT '时间范围',
      score int NOT NULL DEFAULT 0 COMMENT '分数',
      valid tinyint NOT NULL DEFAULT 0 COMMENT '规则是否有效 0:有效 1:无效',
      remark varchar(255) COMMENT '备注',
      PRIMARY KEY (id)
    ) COMMENT '积分规则';

    INSERT INTO dc_ods.integral_rule (id, dim_type, rule_code, rule_desc, time_range, score, remark) VALUES 
    ('1', 'MAC地址', 'RULE_MAC_1', 'MAC地址一致', '历史所有', '5', '是否有模拟MAC行为'),
    ('2', 'MAC地址', 'RULE_MAC_2', 'MAC地址不一致', '历史所有', '-20', null),
    ('3', 'VPN代理', 'RULE_VPN_1', '使用过VPN代理', '历史所有', '-10', '是否使用代理IP行为'),
    ('4', 'VPN代理', 'RULE_VPN_2', '没有使用过VPN代理', '历史所有', '5', null),
    ('5', '手机运行时间', 'RULE_UPTIME_1', '手机运行时间小于1天', '最后一次', '-5', '最近开机到现在的时长'),
    ('6', '手机运行时间', 'RULE_UPTIME_2', '手机运行时间大于3天', '最后一次', '5', null),
    ('7', '手机运行时间', 'RULE_UPTIME_3', '其他情况', '最后一次', '0', null),
    ('8', '手机号码', 'RULE_PHONENUM_1', '有手机号', '历史所有', '5', '移动卡是否能拿到手机号码'),
    ('9', '手机号码', 'RULE_PHONENUM_2', '没有手机号', '历史所有', '0', null),
    ('10', '最低电量', 'RULE_POWER_1', '最低电量没有值', '10天之内', '-10', '每天的最低电量是否有变化'),
    ('11', '最低电量', 'RULE_POWER_2', '最低电量只有一个值', '10天之内', '-5', null),
    ('12', '最低电量', 'RULE_POWER_3', '变化次数小于5', '10天之内', '5', null),
    ('13', '最低电量', 'RULE_POWER_4', '变化次数大于等于5', '10天之内', '10', null),
    ('14', '基站信息', 'RULE_BASESTATION_1', '基站信息没有值', '10天之内', '-10', '联网的基站信息是否有变化'),
    ('15', '基站信息', 'RULE_BASESTATION_2', '基站个数为1', '10天之内', '-5', null),
    ('16', '基站信息', 'RULE_BASESTATION_3', '基站个数大于1，小于5', '10天之内', '5', null),
    ('17', '基站信息', 'RULE_BASESTATION_4', '基站个数大于等于5', '10天之内', '10', null),
    ('18', '亮屏黑屏', 'RULE_LOCKED_1', '亮屏黑屏没有值', '10天之内', '-10', '每天黑屏亮屏次数是否有变化'),
    ('19', '亮屏黑屏', 'RULE_LOCKED_2', '亮屏黑屏只有一个值', '10天之内', '-10', null),
    ('20', '亮屏黑屏', 'RULE_LOCKED_3', '变化次数小于5', '10天之内', '5', null),
    ('21', '亮屏黑屏', 'RULE_LOCKED_4', '变化次数大于等于5', '10天之内', '10', null),
    ('22', 'APP使用', 'RULE_APPUSED_1', '没有使用电话、短信、相机之一', '10天之内', '-10', '手机常用功能用到频率'),
    ('23', 'APP使用', 'RULE_APPUSED_2', '使用次数为1-10', '10天之内', '5', null),
    ('24', 'APP使用', 'RULE_APPUSED_3', '使用次数大于10', '10天之内', '10', null),
    ('25', '网络类型', 'RULE_NETWORKTYPE_1', '网络类型个数大于2', '10天之内', '5', 'wifi,2g,3g,4g是否有切换'),
    ('26', '网络类型', 'RULE_NETWORKTYPE_2', '其他情况', '10天之内', '0', null),
    ('27', '安装卸载', 'RULE_APPINSTALL_1', '安装卸载次数大于3', '10天之内', '5', '是否有卸载安装应用行为'),
    ('28', '安装卸载', 'RULE_APPINSTALL_2', '其他情况', '10天之内', '0', null),
    ('29', '应用账号', 'RULE_APPACCOUNT_1', '应用账号个数大于等于1', '历史所有', '5', '是否有微信，支付宝等账号信息'),
    ('30', '应用账号', 'RULE_APPACCOUNT_2', '其他情况', '历史所有', '0', null),
    ('31', '活跃度', 'RULE_ACTIVE_1', '活跃天数等于1', '10天之内', '-5', null),
    ('32', '活跃度', 'RULE_ACTIVE_2', '活跃天数大于3', '10天之内', '5', '活跃情况'),
    ('33', '活跃度', 'RULE_ACTIVE_3', '其他情况', '10天之内', '0', null);

    CREATE TABLE IF NOT EXISTS dc_ods.user_score_log (
      uuid varchar(32) COMMENT '设备唯一标识',
      rule_id int COMMENT '规则ID',
      create_date date COMMENT '创建日期',
      PRIMARY KEY (uuid, rule_id, create_date)
    ) COMMENT '评分记录';

    CREATE TABLE IF NOT EXISTS dc_ods.user_score (
      uuid varchar(32) COMMENT '设备唯一标识',
      create_date date COMMENT '创建日期',
      score int COMMENT '分数',
      PRIMARY KEY (uuid, create_date)
    ) COMMENT '用户分数';
    " | exec_sql
}

# 导入表
function import_score()
{
    local cur_time=$(date +%s)
    cat > $DATA_DIR/dc_ods/user_score_log_${cur_time}.txt

    echo "LOAD DATA LOCAL INFILE '$DATA_DIR/dc_ods/user_score_log_${cur_time}.txt' REPLACE INTO TABLE dc_ods.user_score_log (uuid, rule_id) 
    SET create_date = $the_date;
    " | exec_sql
}

# 设备其他信息
# 1、MAC地址
# 2、VPN代理
# 3、手机运行时间
# 4、手机号码
function do_other_info()
{
    # 获取数据
    echo "SELECT uuid, changed_mac, used_vpn, uptime, has_phonenum 
    FROM dc_ods.full_other_info;
    " | exec_sql > $DATA_DIR/dc_ods/full_other_info.tmp

    awk -F '\t' 'BEGIN{OFS=FS}{
        # MAC地址
        if($2 == 1){
            print $1,'$RULE_MAC_1'
        }else{
            print $1,'$RULE_MAC_2'
        }

        # VPN代理
        if($3 == 1){
            print $1,'$RULE_VPN_1'
        }else{
            print $1,'$RULE_VPN_2'
        }

        # 手机运行时间
        days=$4/1000/60/60/24
        if(days < 1){
            print $1,'$RULE_UPTIME_1'
        }else if(days > 3){
            print $1,'$RULE_UPTIME_2'
        }else{
            print $1,'$RULE_UPTIME_3'
        }

        # 手机号码
        if($5 == 1){
            print $1,'$RULE_PHONENUM_1'
        }else{
            print $1,'$RULE_PHONENUM_2'
        }
    }' $DATA_DIR/dc_ods/full_other_info.tmp | import_score
}

# 最低电量
function do_power()
{
    # 获取数据
    echo "SELECT uuid, COUNT(DISTINCT power) 
    FROM dc_ods.full_power 
    WHERE create_date >= CURDATE() - INTERVAL $INTERVAL_DAYS DAY 
    GROUP BY 1;
    " | exec_sql > $DATA_DIR/dc_ods/full_power.tmp

    awk -F '\t' 'BEGIN{OFS=FS}{
        if($2 == 0){
            print $1,'$RULE_POWER_1'
        }else if($2 == 1){
            print $1,'$RULE_POWER_2'
        }else if($2 < 5){
            print $1,'$RULE_POWER_3'
        }else if($2 >= 5){
            print $1,'$RULE_POWER_4'
        }
    }' $DATA_DIR/dc_ods/full_power.tmp | import_score
}

# 基站信息
function do_base_station()
{
    # 获取数据
    echo "SELECT uuid, COUNT(DISTINCT station_code) 
    FROM dc_ods.full_base_station 
    WHERE create_date >= CURDATE() - INTERVAL $INTERVAL_DAYS DAY 
    GROUP BY 1;
    " | exec_sql > $DATA_DIR/dc_ods/full_base_station.tmp

    awk -F '\t' 'BEGIN{OFS=FS}{
        if($2 == 0){
            print $1,'$RULE_BASESTATION_1'
        }else if($2 == 1){
            print $1,'$RULE_BASESTATION_2'
        }else if($2 < 5){
            print $1,'$RULE_BASESTATION_3'
        }else if($2 >= 5){
            print $1,'$RULE_BASESTATION_4'
        }
    }' $DATA_DIR/dc_ods/full_base_station.tmp | import_score
}

# 亮屏黑屏
function do_locked()
{
    # 获取数据
    echo "SELECT uuid, COUNT(DISTINCT unlock_count) 
    FROM dc_ods.full_locked 
    WHERE create_date >= CURDATE() - INTERVAL $INTERVAL_DAYS DAY 
    GROUP BY 1;
    " | exec_sql > $DATA_DIR/dc_ods/full_locked.tmp

    awk -F '\t' 'BEGIN{OFS=FS}{
        if($2 == 0){
            print $1,'$RULE_LOCKED_1'
        }else if($2 == 1){
            print $1,'$RULE_LOCKED_2'
        }else if($2 < 5){
            print $1,'$RULE_LOCKED_3'
        }else if($2 >= 5){
            print $1,'$RULE_LOCKED_4'
        }
    }' $DATA_DIR/dc_ods/full_locked.tmp | import_score
}

# APP使用情况
function do_app_used()
{
    # 获取数据
    echo "SELECT uuid, SUM(app_used_count) 
    FROM dc_ods.full_app_used 
    WHERE create_date >= CURDATE() - INTERVAL $INTERVAL_DAYS DAY 
    GROUP BY 1;
    " | exec_sql > $DATA_DIR/dc_ods/full_app_used.tmp

    awk -F '\t' 'BEGIN{OFS=FS}{
        if($2 == 0){
            print $1,'$RULE_APPUSED_1'
        }else if($2 < 10){
            print $1,'$RULE_APPUSED_2'
        }else if($2 >= 10){
            print $1,'$RULE_APPUSED_3'
        }
    }' $DATA_DIR/dc_ods/full_app_used.tmp | import_score
}

# 安装卸载
function do_app_install()
{
    # 获取数据
    echo "SELECT uuid, SUM(app_install_count) 
    FROM dc_ods.full_app 
    WHERE create_date >= CURDATE() - INTERVAL $INTERVAL_DAYS DAY 
    GROUP BY 1;
    " | exec_sql > $DATA_DIR/dc_ods/full_app.tmp

    awk -F '\t' 'BEGIN{OFS=FS}{
        if($2 > 3){
            print $1,'$RULE_APPINSTALL_1'
        }else{
            print $1,'$RULE_APPINSTALL_2'
        }
    }' $DATA_DIR/dc_ods/full_app.tmp | import_score
}

# 应用账号
function do_app_account()
{
    # 获取数据
    echo "SELECT uuid, COUNT(CONCAT(acc_name, acc_type)) 
    FROM dc_ods.full_app_account 
    GROUP BY 1;
    " | exec_sql > $DATA_DIR/dc_ods/full_app_account.tmp

    awk -F '\t' 'BEGIN{OFS=FS}{
        if($2 > 0){
            print $1,'$RULE_APPACCOUNT_1'
        }else{
            print $1,'$RULE_APPACCOUNT_2'
        }
    }' $DATA_DIR/dc_ods/full_app_account.tmp | import_score
}

# 网络类型
function do_network_type()
{
    echo "SELECT uuid, COUNT(DISTINCT link_type) 
    FROM dc_ods.full_network_type 
    WHERE create_date >= CURDATE() - INTERVAL $INTERVAL_DAYS DAY 
    GROUP BY 1;
    " | exec_sql > $DATA_DIR/dc_ods/full_network_type.tmp

    awk -F '\t' 'BEGIN{OFS=FS}{
        if($2 > 2){
            print $1,'$RULE_NETWORKTYPE_1'
        }else{
            print $1,'$RULE_NETWORKTYPE_2'
        }
    }' $DATA_DIR/dc_ods/full_network_type.tmp | import_score
}

# 活跃度
function do_active()
{
    echo "SELECT uuid, COUNT(1) 
    FROM dc_ods.full_active_user 
    WHERE create_date >= CURDATE() - INTERVAL $INTERVAL_DAYS DAY 
    GROUP BY 1;
    " | exec_sql > $DATA_DIR/dc_ods/full_active.tmp

    awk -F '\t' 'BEGIN{OFS=FS}{
        if($2 == 1){
            print $1,'$RULE_ACTIVE_1'
        }else if($2 > 3){
            print $1,'$RULE_ACTIVE_2'
        }else{
            print $1,'$RULE_ACTIVE_3'
        }
    }' $DATA_DIR/dc_ods/full_active.tmp | import_score
}

# 评分
function score()
{
    # 设备其他信息
    log "Do device other info"
    do_other_info

    # 最低电量
    log "Do power"
    do_power

    # 基站信息
    log "Do base station"
    do_base_station

    # 亮屏黑屏
    log "Do locked"
    do_locked

    # APP使用
    log "Do app used"
    do_app_used

    # 安装卸载
    log "Do app install"
    do_app_install

    # 应用账号
    log "Do app account"
    do_app_account

    # 网络类型
    log "Do network type"
    do_network_type

    # 活跃度
    log "Do active"
    do_active
}

# 汇总
function sum()
{
    echo "REPLACE INTO dc_ods.user_score (uuid, create_date, score) 
    SELECT a.uuid, a.create_date, SUM(b.score) 
    FROM dc_ods.user_score_log a 
    INNER JOIN dc_ods.integral_rule b 
    ON a.rule_id = b.id 
    AND a.create_date = $the_date
    GROUP BY 1,2;
    " | exec_sql
}

function etl()
{
    # 创建目录
    mkdir -p $DATA_DIR/dc_ods

    # 评分
    log "Score begin"
    score
    log "Score end"

    # 汇总
    log "Sum user score"
    sum
}

function execute()
{
    log "Create tables"
    create_tables

    log "Do etl"
    etl
}

function main()
{
    log "Current working directory: $BASE_DIR, invoke script: $0 $@"

    the_date=$(date +'%Y%m%d')

    execute

    log "All done"
}
main "$@"