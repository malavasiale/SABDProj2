#!/bin/bash
hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
hdfs dfs -mkdir /results
hdfs dfs -mkdir /input
hdfs dfs -chmod 777 /
hdfs dfs -chmod 777 /input
hdfs dfs -chmod 777 /results
sudo $NIFI_HOME/bin/nifi.sh start
