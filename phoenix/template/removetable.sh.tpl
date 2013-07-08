#!/bin/bash

time=`date -d "now" +%Y%m%d%H%M%S`
LOGFILE=removetable_$time.log

ZKHOST=ZKADDRESS_PLACEHOLDER
ZKPORT=ZKPORT_PLACEHOLDER

echo "ZOOKEEPER HOST =" $ZKHOST
echo "ZooKeeper port =" $ZKPORT

echo "Log to file : " $LOGFILE

echo "*** Make sure that you already drop table in HBase ***"
echo "Removing table..." | tee -a $LOGFILE
cd bin
./psql.sh $ZKHOST:$ZKPORT ../scripts/removetable.sql | tee -a $LOGFILE

echo "Done" | tee -a $LOGFILE

