#!/bin/bash
pid=$(< /data/spark_twitter.pid)
kill $pid
