Overview
========

This document describes how to run LinkBench with Phoenix from Source code.

You can reference the following step to do it step by step. or you can just Download the 
prebuilt binary package and following the README in that package to make your life more easy.
Or you can build your own binary package by reference to the last section of this doc.

-----------------------------

You will need to read README.md first to prepare yourself with
common LinkBench knowledge than come back to here for Phoenix
specific infomation.


Running a Benchmark with Phoenix
==============================
In this section we will document the process of setting
up of Phoenix tables and running a benchmark with LinkBench.

Build LinkBench with Phoenix support
====================================

You will first need to build and install phoenix, and ensure it works upon hbase.
Check out the detail on https://github.com/forcedotcom/phoenix.

After phoenix (current you should use the trunk code which version will be 1.3-SNAPSHOT)
been installed. build LinkBench with phoenix profile enabled.

    mvn clean package -Pphoenix

HBase requirements
------------------

Though phoenix might only require hbase version 0.94.7, there is a bug that impact the
performance a lot which got fixed in 0.94.9+, check out following issue for more detail:

https://issues.apache.org/jira/browse/HBASE-8639


Deployment and config
=====================



Phoenix Tables Setup
--------------------

We need to create tables on the Phoenix.

We'll create the needed tables to store graph nodes, links and link counts.
Run following DDLs in a sql file with phoenix bin/psql.sh check out phoenix
docs for more detail on how to do this.

Noticed that you might need to adjust the pre-split point of the table by change
the "SPLIT ON" values according to how many node you want to load into the table
and how big your cluster is, you don't want to put either too many or too few data
into a single region.

And choose COMPRESSION according to your HBASE cluster capability.

CREATE TABLE linktable (
    id1 BIGINT NOT NULL,
    id2 BIGINT NOT NULL,
    link_type BIGINT NOT NULL,
    visibility INTEGER NOT NULL,
    data VARBINARY(255) NOT NULL,
    time BIGINT NOT NULL,
    version INTEGER NOT NULL
    CONSTRAINT pk PRIMARY KEY (id1, id2, link_type)
    )
    COMPRESSION='LZO'
    SPLIT ON (2500000L, 5000000L, 7500000L, 10000000L, 12500000L, 15000000L, 17500000L, 20000000L, 22500000L, 25000000L, 27500000L, 30000000L, 32500000L, 35000000L, 37500000L, 40000000L, 42500000L, 45000000L, 47500000L);

CREATE TABLE nodetable (
    id BIGINT NOT NULL PRIMARY KEY,
    type INTEGER NOT NULL,
    version BIGINT NOT NULL,
    time INTEGER NOT NULL,
    data VARBINARY NOT NULL
    )
    COMPRESSION='LZO'
    SPLIT ON (2500000L, 5000000L, 7500000L, 10000000L, 12500000L, 15000000L, 17500000L, 20000000L, 22500000L, 25000000L, 27500000L, 30000000L, 32500000L, 35000000L, 37500000L, 40000000L, 42500000L, 45000000L, 47500000L);

CREATE TABLE counttable (
    id BIGINT NOT NULL,
    link_type BIGINT NOT NULL,
    count BIGINT NOT NULL,
    time BIGINT NOT NULL,
    version BIGINT NOT NULL
    CONSTRAINT pk PRIMARY KEY (id, link_type)
    )
    COMPRESSION='LZO'
    SPLIT ON (5000000L, 10000000L, 15000000L, 20000000L, 25000000L, 30000000L, 35000000L, 40000000L, 45000000L);


Configuration Files
-------------------
Check out README.md for gernal configuration files. And you should use
LinkConfigPhoenix.properties instead for reference.

    cp config/LinkConfigPhoenix.properties config/MyConfig.properties

Open MyConfig.properties.  At a minimum you will need to fill in the
settings under *Phoenix Connection Information* to match the server, user
and database you set up earlier. E.g.

    # Phoenix connection information
    host = your_host
    user = your_user
    password = your_password
    port = 2181


Launching Test
==============

Add HBASE and phoenix related jar into classpath in bin/linkbench e.g.

    CLASSPATH=${CLASSPATH}:`~/hbase/bin/hbase classpath`

Or if your client is running on a machine there are no existing HBase,
you will need to copy the hbase jar into classpath.

Now, you can run load and request test with bin/linkbench in linkbench dir e.g.

Loading data : ./bin/linkbench -c config/MyConfig.properties -l -L load.log
Run quest : ./bin/linkbench -c config/MyConfig.properties -r -L request.log


Tunning
=======

The benchmark performance could be impacted by both linkbench, phoenix and hbase cluster settings.

Especially those Concurrent settings, HEAP size, compact strategy, HBase caching, handlers number etc.
They could have huge impact on the performance results. You might probably need to adjust them
according to your cluster size, table size etc.

Also be careful about the GC behaviour, since you are probably running a lot of threads concurrently
over a huge table which probably use memory intensively, thus not only hbase GC behaviour, but also
linkbench client side GC behaviour might impact the benchmark a lot. Make sure your client don't get
blocked by endless GC by tuning related JVM parameters carefully.

Reference https://github.com/forcedotcom/phoenix/wiki/Tuning for tunning phoenix.


Packaging into standalone packages
==================================

Other than the deploy method described above, you can also make a standalone package for distribution.
After you have both phoenix and linkbench build as above. You can skip all the other steps later,
instead in linkbench dir, you can run 

./package_linkbench.sh linkbench_source_dir phoenix_source_dir

Make sure you have both of them built. this scripts just package the necessary file into a final package,
While it will not try to compile either of them for you.

This will generate linkbench_bin_all.tar.bz2 , distribute it to client machine, untar and follow the README in
that package to run it up.

