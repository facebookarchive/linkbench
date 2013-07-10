#!/bin/bash

time=`date -d "now" +%Y%m%d%H%M%S`
LOGFILE=createtable_$time.log

ZKHOST=ZKADDRESS_PLACEHOLDER
ZKPORT=ZKPORT_PLACEHOLDER

echo "ZOOKEEPER HOST =" $ZKHOST
echo "ZooKeeper port =" $ZKPORT

echo "Log to file : " $LOGFILE

echo "Creating table..." | tee -a $LOGFILE
cd bin
./psql.sh $ZKHOST:$ZKPORT ../scripts/createtable.sql | tee -a $LOGFILE

echo "Done" | tee -a $LOGFILE

