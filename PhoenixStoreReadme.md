Overview
========
This document describes how to run LinkBench with Phoenix.
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

After phoenix (current you should use the trunk code which version will be 1.2-SNAPSHOT)
been installed. build LinkBench with phoenix profile enabled.

    mvn clean package -Pphoenix

Phoenix Tables Setup
--------------------

We need to create tables on the Phoenix.

We'll create the needed tables to store graph nodes, links and link counts.
Run the following commands in a sql file with phoenix bin/psql.sh check out phoenix
docs for more detail on how to do this.

    CREATE TABLE linktable (
        id1 BIGINT NOT NULL,
        id2 BIGINT NOT NULL,
        link_type BIGINT NOT NULL,
        visibility INTEGER NOT NULL,
        data VARBINARY(255) NOT NULL,
        time BIGINT NOT NULL,
        version INTEGER NOT NULL
        CONSTRAINT pk PRIMARY KEY (id1, id2, link_type)
        );

    CREATE TABLE counttable (
        id BIGINT NOT NULL,
        link_type BIGINT NOT NULL,
        count BIGINT NOT NULL,
        time BIGINT NOT NULL,
        version BIGINT NOT NULL
        CONSTRAINT pk PRIMARY KEY (id, link_type)
        );
                        
    CREATE TABLE nodetable (
        id BIGINT NOT NULL PRIMARY KEY,
        type INTEGER NOT NULL,
        version BIGINT NOT NULL,
        time INTEGER NOT NULL,
        data VARBINARY NOT NULL
        );



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
--------------

Add HBASE related classpath into bin/linkbench e.g.
CLASSPATH=${CLASSPATH}:`~/hbase/bin/hbase classpath`

Now, you can run load and request test with bin/linkbench
