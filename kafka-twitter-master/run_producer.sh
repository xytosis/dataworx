#!/bin/bash
cd /home/ubuntu/connectx-ml/kafka-twitter-master/
./gradlew run -Pargs="conf/producer.conf" & echo $! > /data/producer.pid
