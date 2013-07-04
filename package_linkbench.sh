#!/bin/bash

if [ $# -lt 2 ]; then
  echo "USAGE:" $0 "LINKBENCH_PATH PHOENIX_PATH"
  exit
fi

DESTPACKAGE=linkbench_bin_all
DESTDIR=`pwd`/${DESTPACKAGE}
echo "LINKBENCH PACKAGE DEST DIR : " ${DESTDIR}

LINKBENCHDIR=$1
PHOENIXDIR=$2

echo "PHOENIXDIR : " $PHOENIXDIR
echo "LINKBENCHDIR : " $LINKBENCHDIR

echo "remove existing destination dir"
rm -rRf ${DESTDIR}

echo "create destination dir"

BINDIR=${DESTDIR}/bin
LIBDIR=${DESTDIR}/lib
CONFIGDIR=${DESTDIR}/config

mkdir -p ${BINDIR}
mkdir -p ${LIBDIR}
mkdir -p ${CONFIGDIR}


echo "copying files into destination dir"
cp -a ${LINKBENCHDIR}/bin/* ${BINDIR}/
cp -a ${LINKBENCHDIR}/config/* ${CONFIGDIR}/
cp -a ${LINKBENCHDIR}/target/FacebookLinkBench.jar ${LIBDIR}/

cp -a ${LINKBENCHDIR}/phoenix/* ${DESTDIR}

cp -a ${PHOENIXDIR}/bin/* ${BINDIR}/
cp -a ${PHOENIXDIR}/target/*-client.jar ${LIBDIR}/

echo "Packaging files into ${DESTPACKAGE}.tar.bz2 ..."
tar -cjf ${DESTPACKAGE}.tar.bz2 ${DESTPACKAGE}

echo "Done"
