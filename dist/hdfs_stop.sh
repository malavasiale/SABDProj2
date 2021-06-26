#!/bin/bash
$HADOOP_HOME/sbin/stop-dfs.sh
sudo $NIFI_HOME/bin/nifi.sh stop
