# 导出keys
./src/redis-cli -a foobared keys "tip-*" > keys

# 统计每个key条数
while read key; do
    echo -e "$key\t\c"
    ./src/redis-cli -a foobared llen $key
done < keys > keylen
