#!/bin/bash

cd /home/ubuntu/connectx-ml/twitter_classifier/scala/

/home/ubuntu/apps/spark-1.4.1/bin/spark-submit \
 --class "com.velankani.apps.connectxml.TwitterTimelineCheck"  \
--master "local[*]" \
 target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
 TWITTER KEY \
 TWITTER SECRET \
 ACCESS \
 ACCESS SECRET
