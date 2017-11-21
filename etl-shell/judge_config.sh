# 真假判断 配置信息


# 统计几天内的数据
INTERVAL_DAYS=10

# 积分规则ID
RULE_MAC_1=1                      # MAC地址一致
RULE_MAC_2=2                      # MAC地址不一致
RULE_VPN_1=3                      # 使用过VPN代理
RULE_VPN_2=4                      # 没有使用过VPN代理
RULE_UPTIME_1=5                      # 手机运行时间小于1天
RULE_UPTIME_2=6                      # 手机运行时间大于3天
RULE_UPTIME_3=7                      # 其他情况
RULE_PHONENUM_1=8                      # 有手机号
RULE_PHONENUM_2=9                      # 没有手机号
RULE_POWER_1=10                      # 最低电量没有值
RULE_POWER_2=11                      # 最低电量只有一个值
RULE_POWER_3=12                      # 变化次数小于5
RULE_POWER_4=13                      # 变化次数大于等于5
RULE_BASESTATION_1=14                      # 基站信息没有值
RULE_BASESTATION_2=15                      # 基站个数为1
RULE_BASESTATION_3=16                      # 基站个数大于1，小于5
RULE_BASESTATION_4=17                      # 基站个数大于等于5
RULE_LOCKED_1=18                      # 亮屏黑屏没有值
RULE_LOCKED_2=19                      # 亮屏黑屏只有一个值
RULE_LOCKED_3=20                      # 变化次数小于5
RULE_LOCKED_4=21                      # 变化次数大于等于5
RULE_APPUSED_1=22                      # 没有使用电话、短信、相机之一
RULE_APPUSED_2=23                      # 使用次数为1-10
RULE_APPUSED_3=24                      # 使用次数大于10
RULE_NETWORKTYPE_1=25                      # 网络类型个数大于2
RULE_NETWORKTYPE_2=26                      # 其他情况
RULE_APPINSTALL_1=27                      # 安装卸载次数大于3
RULE_APPINSTALL_2=28                      # 其他情况
RULE_APPACCOUNT_1=29                      # 应用账号个数大于等于1
RULE_APPACCOUNT_2=30                      # 其他情况
RULE_ACTIVE_1=31                      # 活跃天数等于1
RULE_ACTIVE_2=32                      # 活跃天数大于3
RULE_ACTIVE_3=33                      # 其他情况
RULE_UUID_1=34                   # UUID状态为B
RULE_UUID_2=35                   # UUID状态为C
RULE_UUID_3=36                   # UUID状态为G
RULE_UUID_4=37                   # 其他状态

function replace_var()
{
    sed "s/#the_day#/${the_day}/g;s/#prev_day#/${prev_day}/g;s/#next_day#/${next_day}/g;s/#run_time#/${run_time}/g" |
    sed "s/#INTERVAL_DAYS#/${INTERVAL_DAYS}/g" |
    sed "s/#RULE_MAC_1#/${RULE_MAC_1}/g;s/#RULE_MAC_2#/${RULE_MAC_2}/g" |
    sed "s/#RULE_VPN_1#/${RULE_VPN_1}/g;s/#RULE_VPN_2#/${RULE_VPN_2}/g" |
    sed "s/#RULE_UPTIME_1#/${RULE_UPTIME_1}/g;s/#RULE_UPTIME_2#/${RULE_UPTIME_2}/g;s/#RULE_UPTIME_3#/${RULE_UPTIME_3}/g" |
    sed "s/#RULE_PHONENUM_1#/${RULE_PHONENUM_1}/g;s/#RULE_PHONENUM_2#/${RULE_PHONENUM_2}/g" |
    sed "s/#RULE_POWER_1#/${RULE_POWER_1}/g;s/#RULE_POWER_2#/${RULE_POWER_2}/g;s/#RULE_POWER_3#/${RULE_POWER_3}/g;s/#RULE_POWER_4#/${RULE_POWER_4}/g" |
    sed "s/#RULE_BASESTATION_1#/${RULE_BASESTATION_1}/g;s/#RULE_BASESTATION_2#/${RULE_BASESTATION_2}/g;s/#RULE_BASESTATION_3#/${RULE_BASESTATION_3}/g;s/#RULE_BASESTATION_4#/${RULE_BASESTATION_4}/g" |
    sed "s/#RULE_LOCKED_1#/${RULE_LOCKED_1}/g;s/#RULE_LOCKED_2#/${RULE_LOCKED_2}/g;s/#RULE_LOCKED_3#/${RULE_LOCKED_3}/g;s/#RULE_LOCKED_4#/${RULE_LOCKED_4}/g" |
    sed "s/#RULE_APPUSED_1#/${RULE_APPUSED_1}/g;s/#RULE_APPUSED_2#/${RULE_APPUSED_2}/g;s/#RULE_APPUSED_3#/${RULE_APPUSED_3}/g" |
    sed "s/#RULE_NETWORKTYPE_1#/${RULE_NETWORKTYPE_1}/g;s/#RULE_NETWORKTYPE_2#/${RULE_NETWORKTYPE_2}/g" |
    sed "s/#RULE_APPINSTALL_1#/${RULE_APPINSTALL_1}/g;s/#RULE_APPINSTALL_2#/${RULE_APPINSTALL_2}/g" |
    sed "s/#RULE_APPACCOUNT_1#/${RULE_APPACCOUNT_1}/g;s/#RULE_APPACCOUNT_2#/${RULE_APPACCOUNT_2}/g" |
    sed "s/#RULE_ACTIVE_1#/${RULE_ACTIVE_1}/g;s/#RULE_ACTIVE_2#/${RULE_ACTIVE_2}/g;s/#RULE_ACTIVE_3#/${RULE_ACTIVE_3}/g" |
    sed "s/#RULE_UUID_1#/${RULE_UUID_1}/g;s/#RULE_UUID_2#/${RULE_UUID_2}/g;s/#RULE_UUID_3#/${RULE_UUID_3}/g;s/#RULE_UUID_4#/${RULE_UUID_4}/g"
}