#!/bin/bash

echo "Make sure that you run this scripts inside bin dir"

source config.sh

sed s/ZKADDRESS_PLACEHOLDER/$ZKHOST/g ../template/LinkConfigPhoenix.properties.tpl | sed s/ZKPORT_PLACEHOLDER/$ZKPORT/g > ../config/LinkConfigPhoenix.properties

sed s/ZKADDRESS_PLACEHOLDER/$ZKHOST/g ../template/LinkConfigPhoenixMR.properties.tpl | sed s/ZKPORT_PLACEHOLDER/$ZKPORT/g > ../config/LinkConfigPhoenixMR.properties

sed s/ZKADDRESS_PLACEHOLDER/$ZKHOST/g ../template/loaddata.sh.tpl | sed s/ZKPORT_PLACEHOLDER/$ZKPORT/g > ../loaddata.sh
chmod ug+x ../loaddata.sh

sed s/ZKADDRESS_PLACEHOLDER/$ZKHOST/g ../template/inittable.sh.tpl | sed s/ZKPORT_PLACEHOLDER/$ZKPORT/g > ../inittable.sh
chmod ug+x ../inittable.sh

sed s/ZKADDRESS_PLACEHOLDER/$ZKHOST/g ../template/removetable.sh.tpl | sed s/ZKPORT_PLACEHOLDER/$ZKPORT/g > ../removetable.sh
chmod ug+x ../removetable.sh

sed s/ZKADDRESS_PLACEHOLDER/$ZKHOST/g ../template/doquery.sh.tpl | sed s/ZKPORT_PLACEHOLDER/$ZKPORT/g > ../doquery.sh
chmod ug+x ../doquery.sh

sed -i s#.*HBASE_JAR_FILE=.*#HBASE_JAR_FILE=$HBASE_JAR#g psql.sh
sed -i s#.*phoenix_jar_path=.*#phoenix_jar_path=\"..\/lib\"#g psql.sh

sed s/MAXID1_PLACEHOLDER/$MAXID1/g ../template/PhoenixWorkload.properties.tpl > ../config/PhoenixWorkload.properties

sed s/MAXID1_PLACEHOLDER/$MAXID1/g ../template/generateSQL.sh.tpl > ../scripts/generateSQL.sh
chmod ug+x ../scripts/generateSQL.sh
../scripts/generateSQL.sh ../scripts/createtable.sql

echo "Config changes applied."
