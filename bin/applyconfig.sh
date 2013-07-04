#!/bin/bash

source config.sh

sed s/ZKADDRESS_PLACEHOLDER/$ZKHOST/g ../template/LinkConfigPhoenix.properties.tpl | sed s/ZKPORT_PLACEHOLDER/$ZKPORT/g > ../config/LinkConfigPhoenix.properties

sed s/ZKADDRESS_PLACEHOLDER/$ZKHOST/g ../template/LinkConfigPhoenixMR.properties.tpl | sed s/ZKPORT_PLACEHOLDER/$ZKPORT/g > ../config/LinkConfigPhoenixMR.properties

sed s/ZKADDRESS_PLACEHOLDER/$ZKHOST/g ../template/loaddata.sh.tpl | sed s/ZKPORT_PLACEHOLDER/$ZKPORT/g > ../loaddata.sh
chmod ug+x ../loaddata.sh

sed s/ZKADDRESS_PLACEHOLDER/$ZKHOST/g ../template/doquery.sh.tpl | sed s/ZKPORT_PLACEHOLDER/$ZKPORT/g > ../doquery.sh
chmod ug+x ../doquery.sh


sed s/MAXID1_PLACEHOLDER/$MAXID1/g ../template/PhoenixWorkload.properties.tpl > ../config/PhoenixWorkload.properties

sed s/MAXID1_PLACEHOLDER/$MAXID1/g ../template/generateSQL.sh.tpl > ../scripts/generateSQL.sh
chmod ug+x ../scripts/generateSQL.sh
../scripts/generateSQL.sh ../scripts/createtable.sql

