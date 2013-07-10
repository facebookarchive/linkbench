#!/bin/bash

time=`date -d "now" +%Y%m%d%H%M%S`
LOGFILE=query_$time.log

ZKHOST=ZKADDRESS_PLACEHOLDER
ZKPORT=ZKPORT_PLACEHOLDER

echo "ZOOKEEPER HOST =" $ZKHOST
echo "ZooKeeper port =" $ZKPORT

echo "Log to file : " $LOGFILE

echo "run Query..." | tee -a $LOGFILE

./bin/linkbench -c config/LinkConfigPhoenix.properties -r -L $LOGFILE

echo "Done" | tee -a $LOGFILE

