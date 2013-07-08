#!/bin/bash

# ------------------------------------------------------------
# This section of value when changed, need to run applyconfig.sh to reflect the changes.

# HBase Zookeeper location.

ZKHOST=localhost
ZKPORT=2181

# How many nodes the table will have, 1M Node will lead to around 1GB data
MAXID1=50000001

#--------------------------------------------------------------

# the location of HBase jar
# if there is no hbase on local machine, copy the jar from the HBase cluster.
# Please fill in full path.
HBASE_JAR=/hbase/hbase-0.94.7.jar

JAVA_HOME=/usr/java/jdk1.7.0_04

