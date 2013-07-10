#!/bin/bash

time=`date -d "now" +%Y%m%d%H%M%S`
LOGFILE=load_$time.log

ZKHOST=ZKADDRESS_PLACEHOLDER
ZKPORT=ZKPORT_PLACEHOLDER

echo "ZOOKEEPER HOST =" $ZKHOST
echo "ZooKeeper port =" $ZKPORT

echo "Log to file : " $LOGFILE

echo "Loading data..." | tee -a $LOGFILE

./bin/linkbench -c config/LinkConfigPhoenix.properties -l -L $LOGFILE

echo "Done" | tee -a $LOGFILE

