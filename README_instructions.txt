Prerequisites:
--------------

Java: You should set the JAVA_HOME environment variable to the location
of a Java 6 runtime environment.  For example:
    // where jdk is installed
    export JAVA_HOME="/usr/local/jdk-6u22-64"

Ant: To build LinkBench, you will need the Apache Ant build tool. If
    you do not have it already, it is available from:
    http://ant.apache.org

MySql Connector:  A version of the MySql connector is bundled with
    LinkBench.  If you wish to use a more recent version, remove the
    mysql jar under lib and replace it with a new version. See
    http://dev.mysql.com/downloads/connector/j/

MySql Instance: If you wish to run the MySql benchmark you will need
    a running MySql installation with free disk space.

Building LinkBench:
-------------------
To compile LinkBench from scratch:
    ant clean dist


Setting up MySql Database:
--------------------------
You will need to create a new database and tables in MySql.  Execute the
following statements. Note: we are not taking security/permissions into
account, assuming you will connect to MySql as a user with administrator
privileges.

MySql configuration hint: you will want to set the buffer size in the
    MySql configuration file to at least 1GB.  You will have to restart
    the MySql daemon for this to take effect.

Execute the following commands in the database:

    create database linkdb;
    use linkdb;

    CREATE TABLE `linktable` (
      `id1` bigint(20) unsigned NOT NULL DEFAULT '0',
      `id1_type` int(10) unsigned NOT NULL DEFAULT '0',
      `id2` bigint(20) unsigned NOT NULL DEFAULT '0',
      `id2_type` int(10) unsigned NOT NULL DEFAULT '0',
      `link_type` bigint(20) unsigned NOT NULL DEFAULT '0',
      `visibility` tinyint(3) NOT NULL DEFAULT '0',
      `data` varchar(255) NOT NULL DEFAULT '',
      `time` bigint(20) unsigned NOT NULL DEFAULT '0',
      `version` int(11) unsigned NOT NULL DEFAULT '0',
      PRIMARY KEY (`id1`,`id2`,`link_type`),
      KEY `id2_vis` (`id2`,`visibility`),
      KEY `id1_type` (`id1`,`link_type`,`visibility`,`time`,`version`,`data`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

    CREATE TABLE `counttable` (
      `id` bigint(20) unsigned NOT NULL DEFAULT '0',
      `id_type` int(10) unsigned NOT NULL DEFAULT '0',
      `link_type` bigint(20) unsigned NOT NULL DEFAULT '0',
      `count` int(10) unsigned NOT NULL DEFAULT '0',
      `time` bigint(20) unsigned NOT NULL DEFAULT '0',
      `version` bigint(20) unsigned NOT NULL DEFAULT '0',
      PRIMARY KEY (`id`,`link_type`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

Configuring LinkBench:
----------------------
Examine LinkConfigMysql.properties under the config directory and change 
values as needed. At a minimum you need to set  username, password of
account to use for accessing mysql and set dbid to database that you
want to use.

Loading LinkBench Data:
-----------------------
In the MySql shell, you can make these optional temporary changes to
speed up the loading process:

    mysql> alter table linktable drop key `id2_vis`, drop key `id1_type`;
           set global innodb_flush_log_at_trx_commit = 2;
           set global sync_binlog = 0;

Other MySql settings will affect loading speed.  The larger the InnoDB
buffer pool the better.  Two settings that can make a substantial
difference for inserts are innodb_log_file_size, which should
be set to a reasonably large value (25%-100% of buffer pool) and
innodb_log_buffer_size, which you should check to see that it is
sufficiently large (e.g 64M or more is plenty).


Now, in your shell, you can execute the loading phase of linkbench using
linkbench with the -l switch.
    # LOAD DATA . takes about 11000 seconds.
    ./bin/linkbench -l

When loading finishes, You will see a message like this:
    LOAD PHASE COMPLETED. Expected to load 10000000 links. 48181609 loaded in 10971 seconds.Links/second = 4391

At this stage, you can check the size of the MySql files on disk to
make sure that data loaded correctly.
    # Check size of ibd files (total should be close to 10G approx)
    ls -l /data/mysql/linkdb/linktable.ibd
    ls -l /data/mysql/linkdb/counttable.ibd

IF YOU PERFORMED THE PRIOR OPTIMIZATIONS FOR LOADING, REVERT THESE NOW:
    mysql> set global innodb_flush_log_at_trx_commit = 1;
           set global sync_binlog = 1;
           alter table linktable add key `id2_vis` (`id2`,`visibility`), add key `id1_type`(`id1`,`link_type`,`visibility`,`time`,`version`,`data`);

The last alter table statement will take about 30 minutes.  Once it
finishes you can check the on-disk size of the MySql database:
    # Check size of ibd file (total should be close to 20G approx)
    ls -l /data/mysql/linkdb/linktable.ibd
    ls -l /data/mysql/linkdb/counttable.ibd

Running the LinkBench Request Benchmark!
----------------------------------------
To run the benchmark, execute linkbench with the -r switch. Repeat this step a few times and see if the numbers are consistent.
    ./bin/linkbench -r

The last line of output should look something like this:
    REQUEST PHASE COMPLETED. 38876535 requests done in 1802 seconds.Requests/second = 21573

Also, look at innodb_pages_read and innodb_pages_written during the request phase. I
have seen values of about 750 for the former and about 50 for the latter.
