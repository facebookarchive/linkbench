#!/bin/bash

time=`date -d "now" +%Y%m%d%H%M%S`
LOGFILE=load_$time.log

ZKHOST=ZKADDRESS_PLACEHOLDER
ZKPORT=ZKPORT_PLACEHOLDER

echo "ZOOKEEPER HOST =" $ZKHOST
echo "ZooKeeper port =" $ZKPORT

echo "Log to file : " $LOGFILE

echo "Creating table..." | tee -a $LOGFILE
cd bin
./psql.sh $ZKHOST:$ZKPORT ../scripts/createtable.sql | tee -a $LOGFILE

echo "Loading data..." | tee -a $LOGFILE

cd ..
./bin/linkbench -c config/LinkConfigPhoenix.properties -l -L $LOGFILE

echo "Done" | tee -a $LOGFILE

