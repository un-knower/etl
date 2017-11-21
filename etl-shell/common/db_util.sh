# 数据库操作公共类


# 运行环境判断
if [[ -z "$run_env" ]]; then
    if [[ "$LOCAL_IP" =~ 192.168 ]]; then
        # 测试环境
        run_env="-test"
    fi
elif [[ "$run_env" = "online" ]]; then
    unset $run_env
else
    run_env="-$run_env"
fi
debug "Source config: $ETL_HOME/common/config${run_env}.sh"
source $ETL_HOME/common/config${run_env}.sh


# 配置数据库
function set_db()
{
    db_host="$1"
    db_user="$2"
    db_passwd="$3"
    db_name="$4"
    db_port="${5:-$DEFAULT_MYSQL_PORT}"
    db_charset="${6:-$DEFAULT_MYSQL_CHARSET}"
    db_extras="$7"

    db_url=$(make_mysql_url "$db_host" "$db_user" "$db_passwd" "$db_name" "$db_port" "$db_charset" "$db_extras")
}

# 执行sql语句
function exec_sql()
{
    local sql="$1"
    local extras="$2"

    if [[ -z "$sql" ]]; then
        sql=`cat`
    fi

    local sql_log_file=${log_path:-.}/etl_sql.log

    mysql_executor "SET NAMES $db_charset;$sql" "$db_url $extras"
}