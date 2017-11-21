#!/bin/bash
#
# 渠道评级
# sdk_dw.fact_channel
# 可用任务周期：天


source $ETL_HOME/common/db_util.sh


# 最小用户量
MIN_USER_COUNT=50


# 获取渠道指标
function get_data()
{
    # 设置数据库连接
    set_db ${SDK_ODS[@]}

    echo "SELECT
      a.channel_id,
      a.user_count,
      IFNULL(a.keep_pct_1, 0),
      IFNULL(a.keep_pct_3, 0),
      IFNULL(a.keep_pct_6, 0),
      IFNULL(b.vpn_pct, 0),
      IFNULL(b.phone_pct, 0),
      IFNULL(b.network_pct, 0),
      IFNULL(b.runtime_pct, 0),
      IFNULL(b.unlock_pct, 0),
      IFNULL(b.battery_pct, 0),
      IFNULL(b.etime_pct, 0),
      IFNULL(b.station_pct, 0),
      IFNULL(b.account_pct, 0),
      IFNULL(b.install_pct, 0),
      IFNULL(b.appuse_pct, 0)
    FROM channel_keep a
    INNER JOIN channel_sum b
    ON a.channel_id = b.channel_id;
    " | exec_sql > $data_path/fact_channel.txt

    # 设置数据库连接
    set_db ${SDK_DW[@]}

    # 任务重做
    if [[ $redo_flag -eq 1 ]]; then
        log_task $LOG_LEVEL_INFO "Redo task, delete history data"
        echo "DELETE FROM fact_channel WHERE stat_date = $the_day;" | exec_sql
    fi

    echo "LOAD DATA LOCAL INFILE '$data_path/fact_channel.txt' REPLACE INTO TABLE fact_channel
    (channel_id,user_count,keep_pct_1,keep_pct_3,keep_pct_6,vpn_pct,phone_pct,network_pct,runtime_pct,unlock_pct,battery_pct,etime_pct,station_pct,account_pct,install_pct,appuse_pct)
    SET stat_date=$the_day;
    " | exec_sql
}

# 渠道评级
function rate()
{
    # 设置数据库连接
    set_db ${SDK_DW[@]}

    echo "UPDATE fact_channel
    SET channel_rank = 'A'
    WHERE stat_date = $the_day
    AND user_count > $MIN_USER_COUNT
    AND keep_pct_1 > 0.3
    AND keep_pct_3 > 0.2
    AND keep_pct_6 > 0.1
    AND vpn_pct < 0.05
    AND phone_pct > 0.6
    AND network_pct < 0.6
    AND runtime_pct < 0.6
    AND battery_pct < 0.6
    AND station_pct < 0.6;

    UPDATE fact_channel
    SET channel_rank = 'B'
    WHERE stat_date = $the_day
    AND user_count > $MIN_USER_COUNT
    AND keep_pct_1 > 0.15
    AND keep_pct_3 > 0.1
    AND keep_pct_6 > 0.05
    AND vpn_pct < 0.15
    AND phone_pct > 0.2
    AND network_pct < 0.85
    AND runtime_pct < 0.9
    AND battery_pct < 0.9
    AND station_pct < 0.9
    AND channel_rank IS NULL;

    UPDATE fact_channel
    SET channel_rank = 'C'
    WHERE stat_date = $the_day
    AND user_count > $MIN_USER_COUNT
    AND keep_pct_1 > 0.1
    AND keep_pct_3 > 0.05
    AND keep_pct_6 > 0.02
    AND vpn_pct < 0.3
    AND phone_pct > 0.1
    AND network_pct < 0.95
    AND runtime_pct < 0.95
    AND battery_pct < 0.95
    AND station_pct < 0.95
    AND channel_rank IS NULL;

    UPDATE fact_channel
    SET channel_rank = 'E'
    WHERE stat_date = $the_day
    AND user_count > $MIN_USER_COUNT
    AND (
      keep_pct_1 < 0.05
      OR keep_pct_3 < 0.02
      OR keep_pct_6 < 0.01
    )
    AND (
      vpn_pct > 0.6
      OR phone_pct < 0.05
      OR network_pct > 0.98
      OR runtime_pct > 0.98
      OR battery_pct > 0.98
      OR station_pct > 0.98
    )
    AND channel_rank IS NULL;

    UPDATE fact_channel SET channel_rank = 'D' WHERE stat_date = $the_day AND channel_rank IS NULL;
    " | exec_sql
}

function execute()
{
    # 获取渠道指标
    get_data

    # 渠道评级
    rate
}
execute "$@"