#!/bin/bash

set -x
set -e
if [ $# -ge 1 ]
then
  day=$1
else
    day=`date -d "-1day" "+%Y-%m-%d"`
fi

DIR=$(cd "$(dirname "$0")"; pwd)

HDFSDATE=`date "+%Y%m%d%H%M"`
HBASEDATE=`date "+%Y-%m-%d"`


echo "you have 5 seconds to about!"
sleep 5

##删除旧分词数据
  hadoop fs -rm -f /user/data/ckooc-ml/fenci ||echo ""

##V4 分词  数据存储目录有 /user/data/ckooc-ml/fenci/*  和   /user/data/test/tmp1 /user/data/test/tmp2
spark-submit \
--class com.jm.IDFTagsTrain \
--master yarn \
--deploy-mode cluster \
--jars $DIR/json-lib-2.4-jdk15.jar,$DIR/nlp-lang-1.7.jar,$DIR/hanlp-1.6.8.jar,$DIR/ansj_seg-5.0.0.jar,$DIR/jieba-analysis-1.0.0.jar \
--driver-memory 4G \
--driver-cores 3 \
--executor-memory 4G \
--num-executors 4 \
--conf spark.kryoserializer.buffer.max=1000 \
--conf spark.network.timeout=10000000 \
--conf spark.executor.heartbeatInterval=10000000 \
--conf spark.akka.frameSize=1000 \
--conf spark.driver.maxResultSize=4G \
$DIR/newtags.jar 4 $HDFSDATE $HBASEDATE

##删除旧的hive数据
hadoop fs -rm -r /user/chenmeng_test/tags/* || echo ""
hadoop fs -rm -r /user/chenmeng_test/tags_aid_detail/* || echo ""
##导数据到hive
hadoop fs -cp /user/data/test/tmp2 /user/chenmeng_test/tags/ ||echo ""
hadoop fs -cp /user/data/test/tmp1/part-* /user/chenmeng_test/tags_aid_detail/ ||echo ""
















































