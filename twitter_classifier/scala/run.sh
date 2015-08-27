#!/bin/bash
cd /home/ubuntu/connectx-ml/twitter_classifier/scala/
/home/ubuntu/apps/spark-1.4.1/bin/spark-submit \
     --class "com.velankani.apps.connectxml.NaiveBayesPredict" \
     --master YOUR_SPARK_MASTER \
     target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
     ${YOUR_MODEL_DIR:-YOUR_HDFS_MODEL} \
     ${FILE_USED_FOR_TFIDF:-YOUR_HDFS_FILE} \
     ${KAFKA_BROKERS:-YOUR_KAFKA_BROKERS} \
     ${KAFKA_TOPICS:-zerg.hydra} \
     --consumerKey YOUR_KEY \
     --consumerSecret YOUR_SECRET \
     --accessToken YOUR_TOKEN  \
     --accessTokenSecret YOUR_TOKEN_SECRET & echo $! > /data/spark_twitter.pid
