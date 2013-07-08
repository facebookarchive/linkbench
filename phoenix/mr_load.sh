#!/bin/bash

time=`date -d "now" +%Y%m%d%H%M%S`
LOGFILE=MRload_$time.log

./bin/linkbenchMR -files config/LinkConfigPhoenixMR.properties,config/PhoenixWorkload.properties,config/Distribution.dat,config/hbase-site.xml  config/LinkConfigPhoenixMR.properties | tee $LOGFILE
