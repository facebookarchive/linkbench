// where jdk is installed
export JAVA_HOME="/usr/local/jdk-6u22-64"

// At least current dir and mysql connector jar should be in CLASSPATH
// Mysql connector jar can be downloaded from http://dev.mysql.com/downloads/connector/j/.
// Download version 5.0.8 or higher. Copy the mysql jar to the lib directory.
export CLASSPATH=.:<path to mysql-connector-java-5.0.8-bin.jar>

export PATH=$JAVA_HOME/bin:$PATH

// compile
ant clean dist

// create tables in mysql
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

// Examine LinkConfigMysql.properties and change values as needed. At a minimum u need to set
// username, password of account to use for accessing mysql and set dbid to database u want to use.

// Set mysql buffer pool size to 1G and reboot

// OPTIONAL OPTIMIATIONS TO MAKE THE LOAD FAST

// drop secondary indexes
mysql> alter table linktable drop key `id2_vis`, drop key `id1_type`;

set global innodb_flush_log_at_trx_commit = 2;

set global sync_binlog = 0;

// LOAD DATA . takes about 11000 seconds.
java com.facebook.LinkBench.LinkBenchDriver com/facebook/LinkBench/LinkConfigMysql.properties 1

// You will see last line of output like this
LOAD PHASE COMPLETED. Expected to load 10000000 links. 48181609 loaded in 10971 seconds.Links/second = 4391

// Check size of ibd files (total should be close to 10G approx)
ls -l /data/mysql/linkdb/linktable.ibd
ls -l /data/mysql/linkdb/counttable.ibd

// REVERT THE OPTIMIZATIONS DONE FOR LOAD

set global innodb_flush_log_at_trx_commit = 1;

set global sync_binlog = 1;

// this takes about 30 minutes
alter table linktable add key `id2_vis` (`id2`,`visibility`), add key `id1_type`(`id1`,`link_type`,`visibility`,`time`,`version`,`data`);

// Check size of ibd file (total should be close to 20G approx)
ls -l /data/mysql/linkdb/linktable.ibd
ls -l /data/mysql/linkdb/counttable.ibd

// REQUEST PHASE. Repeat this step a few times and see if the numbers are consistent.
java com.facebook.LinkBench.LinkBenchDriver com/facebook/LinkBench/LinkConfigMysql.properties 2

// You will see last line of output something like this
REQUEST PHASE COMPLETED. 38876535 requests done in 1802 seconds.Requests/second = 21573

// Also, look at innodb_pages_read and innodb_pages_written during the request phase. I
// have seen values of about 750 for the former and about 50 for the latter.
