
hdfs dfs -get /flume/advs/2017-05-02


rm -f 2017-05-02/topic_ad_advs/advs171.2017-*
grep -h "2017-04-28" 2017-05-02/topic_ad_advs/* > 2017-05-02/topic_ad_advs/advs171.2017-04-28
grep -h "2017-04-29" 2017-05-02/topic_ad_advs/* > 2017-05-02/topic_ad_advs/advs171.2017-04-29
grep -h "2017-04-30" 2017-05-02/topic_ad_advs/* > 2017-05-02/topic_ad_advs/advs171.2017-04-30
grep -h "2017-05-01" 2017-05-02/topic_ad_advs/* > 2017-05-02/topic_ad_advs/advs171.2017-05-01

rm -f 2017-05-02/topic_ad_reply/advs171.2017-*
grep -h "2017-04-28" 2017-05-02/topic_ad_reply/* > 2017-05-02/topic_ad_reply/advs171.2017-04-28
grep -h "2017-04-29" 2017-05-02/topic_ad_reply/* > 2017-05-02/topic_ad_reply/advs171.2017-04-29
grep -h "2017-04-30" 2017-05-02/topic_ad_reply/* > 2017-05-02/topic_ad_reply/advs171.2017-04-30
grep -h "2017-05-01" 2017-05-02/topic_ad_reply/* > 2017-05-02/topic_ad_reply/advs171.2017-05-01

rm -f 2017-05-02/topic_ad_upgrd/advs171.2017-*
grep -h "2017-04-28" 2017-05-02/topic_ad_upgrd/* > 2017-05-02/topic_ad_upgrd/advs171.2017-04-28
grep -h "2017-04-29" 2017-05-02/topic_ad_upgrd/* > 2017-05-02/topic_ad_upgrd/advs171.2017-04-29
grep -h "2017-04-30" 2017-05-02/topic_ad_upgrd/* > 2017-05-02/topic_ad_upgrd/advs171.2017-04-30
grep -h "2017-05-01" 2017-05-02/topic_ad_upgrd/* > 2017-05-02/topic_ad_upgrd/advs171.2017-05-01

rm -f 2017-05-02/topic_ad_visit/advs171.2017-*
grep -h "2017-04-28" 2017-05-02/topic_ad_visit/* > 2017-05-02/topic_ad_visit/advs171.2017-04-28
grep -h "2017-04-29" 2017-05-02/topic_ad_visit/* > 2017-05-02/topic_ad_visit/advs171.2017-04-29
grep -h "2017-04-30" 2017-05-02/topic_ad_visit/* > 2017-05-02/topic_ad_visit/advs171.2017-04-30
grep -h "2017-05-01" 2017-05-02/topic_ad_visit/* > 2017-05-02/topic_ad_visit/advs171.2017-05-01


hdfs dfs -mkdir -p /flume/advs/2017-04-29/topic_ad_advs
hdfs dfs -mkdir -p /flume/advs/2017-04-29/topic_ad_reply
hdfs dfs -mkdir -p /flume/advs/2017-04-29/topic_ad_upgrd
hdfs dfs -mkdir -p /flume/advs/2017-04-29/topic_ad_visit
hdfs dfs -mkdir -p /flume/advs/2017-04-30/topic_ad_advs
hdfs dfs -mkdir -p /flume/advs/2017-04-30/topic_ad_reply
hdfs dfs -mkdir -p /flume/advs/2017-04-30/topic_ad_upgrd
hdfs dfs -mkdir -p /flume/advs/2017-04-30/topic_ad_visit
hdfs dfs -mkdir -p /flume/advs/2017-05-01/topic_ad_advs
hdfs dfs -mkdir -p /flume/advs/2017-05-01/topic_ad_reply
hdfs dfs -mkdir -p /flume/advs/2017-05-01/topic_ad_upgrd
hdfs dfs -mkdir -p /flume/advs/2017-05-01/topic_ad_visit


hdfs dfs -put 2017-05-02/topic_ad_advs/advs171.2017-04-28 /flume/advs/2017-04-28/topic_ad_advs
hdfs dfs -put 2017-05-02/topic_ad_advs/advs171.2017-04-29 /flume/advs/2017-04-29/topic_ad_advs
hdfs dfs -put 2017-05-02/topic_ad_advs/advs171.2017-04-30 /flume/advs/2017-04-30/topic_ad_advs
hdfs dfs -put 2017-05-02/topic_ad_advs/advs171.2017-05-01 /flume/advs/2017-05-01/topic_ad_advs

hdfs dfs -put 2017-05-02/topic_ad_reply/advs171.2017-04-28 /flume/advs/2017-04-28/topic_ad_reply
hdfs dfs -put 2017-05-02/topic_ad_reply/advs171.2017-04-29 /flume/advs/2017-04-29/topic_ad_reply
hdfs dfs -put 2017-05-02/topic_ad_reply/advs171.2017-04-30 /flume/advs/2017-04-30/topic_ad_reply
hdfs dfs -put 2017-05-02/topic_ad_reply/advs171.2017-05-01 /flume/advs/2017-05-01/topic_ad_reply

hdfs dfs -put 2017-05-02/topic_ad_upgrd/advs171.2017-04-28 /flume/advs/2017-04-28/topic_ad_upgrd
hdfs dfs -put 2017-05-02/topic_ad_upgrd/advs171.2017-04-29 /flume/advs/2017-04-29/topic_ad_upgrd
hdfs dfs -put 2017-05-02/topic_ad_upgrd/advs171.2017-04-30 /flume/advs/2017-04-30/topic_ad_upgrd
hdfs dfs -put 2017-05-02/topic_ad_upgrd/advs171.2017-05-01 /flume/advs/2017-05-01/topic_ad_upgrd

hdfs dfs -put 2017-05-02/topic_ad_visit/advs171.2017-04-28 /flume/advs/2017-04-28/topic_ad_visit
hdfs dfs -put 2017-05-02/topic_ad_visit/advs171.2017-04-29 /flume/advs/2017-04-29/topic_ad_visit
hdfs dfs -put 2017-05-02/topic_ad_visit/advs171.2017-04-30 /flume/advs/2017-04-30/topic_ad_visit
hdfs dfs -put 2017-05-02/topic_ad_visit/advs171.2017-05-01 /flume/advs/2017-05-01/topic_ad_visit
