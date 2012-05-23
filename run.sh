#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# The command script
#
# Environment Variables
#
#   JAVA_HOME        The java implementation to use.  Overrides JAVA_HOME.
#
#   HEAPSIZE  The maximum amount of heap to use, in MB.
#                    Default is 1000.
#
#   OPTS      Extra Java runtime options.
#
#   CONF_DIR  Alternate conf dir. Default is ./config.
#
#

# This script creates the benchmark data and then runs the workload
# on it

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
LINKHOMEDIR=$bin

# get arguments
COMMAND=$1
shift

# some Java parameters
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi

if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx1000m

# check envvars which might override default args
if [ "$HEAPSIZE" != "" ]; then
  #echo "run with heapsize $HEAPSIZE"
  JAVA_HEAP_MAX="-Xmx""$HEAPSIZE""m"
  #echo $JAVA_HEAP_MAX
fi

# CLASSPATH initially contains $CONF_DIR
CLASSPATH="${CONF_DIR}"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar

# so that filenames w/ spaces are handled correctly in loops below
IFS=

# add libs to CLASSPATH
for f in $LINKHOMEDIR/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# add dist to CLASSPATH
for f in $LINKHOMEDIR/dist/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# restore ordinary behaviour
unset IFS

# figure out which class to run
CLASS='com.facebook.LinkBench.LinkBenchDriver'

# command line opts  to run 
if [ "$CMDLINE_OPTS" == "" ]; then
  CMDLINE_OPTS="$LINKHOMEDIR/config/LinkConfigMysql.properties 3"
  echo XXX CMDLINE_OPTS=$CMDLINE_OPTS
fi

# run it
echo "$JAVA" $JAVA_HEAP_MAX $OPTS $JMX_OPTS -classpath "$CLASSPATH" $CLASS $CMDLINE_OPTS "$@"
exec "$JAVA" $JAVA_HEAP_MAX $OPTS $JMX_OPTS -classpath "$CLASSPATH" $CLASS $CMDLINE_OPTS "$@"
