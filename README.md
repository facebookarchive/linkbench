- - -

**_This project is not actively maintained. Proceed at your own risk!_**

- - -  

LinkBench Overview
====================
LinkBench is a database benchmark developed to evaluate database
performance for workloads similar to those of Facebook's production MySQL deployment.
LinkBench is highly configurable and extensible.  It can be reconfigured
to simulate a variety of workloads and plugins can be written for
benchmarking additional database systems.

LinkBench is released under the Apache License, Version 2.0.

Background
----------
One way of modeling social network data is as a *social graph*, where
entities or *nodes* such as people, posts, comments and pages are connected
by *links* which model different relationships between the nodes.  Different
types of links can represent friendship between two users, a user liking another object,
ownership of a post, or any relationship you like.  These nodes and links carry metadata such as
their type, timestamps and version numbers, along with arbitrary *payload data*.

Facebook represents much of its data in this way, with the data stored in MySQL
databases.  The goal of LinkBench is to emulate the social graph database workload
and provide a realistic benchmark for database performance on social workloads.
LinkBench's data model is based on the social graph, and LinkBench has the ability to generate
a large synthetic social graph with key properties similar to the real graph.
The workload of database operations is based on Facebook's production workload, and is also
generated in such a way that key properties of the workload match the production workload.

LinkBench Architecture
----------------------
<pre>
++====================================++
||          LinkBench Driver          ||
++====================================++
||   +---------------------------+    ||
||   | Graph      | Workload     |    ||      Open connections  +=======+
||   | Generator  | Generator    |    ||   /------------------> | Graph |
||   +---------------------------+    ||  /-------------------> | Store |
||   |                           |&lt;======---------------------> | Shard |
||   |   Graph Store Adapter     |&lt;======---------------------> |       |
||   |   (e.g. MySQL adapter)    |&lt;======---------------------> | e.g.  |
||   +---------------------------+    ||  \-------------------> | MySQL |
||                                    ||   \------------------> | Server|
||   ~~~~~~~~~~~~    ~~~~~~~~~~~~     ||                        +=======+
||   ~~~~~~~~~~~~    ~~~~~~~~~~~~     ||
||   ~~~~~~~~~~~~    ~~~~~~~~~~~~     ||
||   ~~~~~~~~~~~~    ~~~~~~~~~~~~     ||
||     Requester Threads              ||
++====================================++
</pre>

The main software component of LinkBench is the driver, which acts as the
client to the database being benchmarked.
LinkBench is designed to support benchmarking of any database system that can support
all of the require graph operations through a *Graph Store Adapter*.

The LinkBench benchmark typically proceeds in two phases.

The first is the *load phase*,
where an initial graph is generated using the
*graph generator* and loaded into the graph store in bulk.
On a large benchmark run, this graph might have a billion nodes, and occupy over a terabyte
on disk.  The generated graph is designed to have similar properties to the Facebook
social graph.  For example, the number of links out from each node follows a power-law
distribution, where most nodes have at most a few links, but a few nodes have many
more links.

The second is the *request phase*, where the actual benchmarking occurs.  In
the request phase, the benchmark driver spawns many request threads, which make
concurrent requests to the database.  The *workload generator* is used by each
request thread to generate a series of database operations that mimics the
Facebook production workload in many aspects.  For example, the mix of
different varieties of read and write operations is the same, and the
access patterns create a similar pattern of hot (frequently access)
and cold nodes in the graph.  At the end of the request phase LinkBench
will report a range of statistics such as latency and throughput.

Getting Started
===============
In this README we'll walk you through compiling LinkBench and running
a MySQL benchmark.

Prerequisites:
--------------
These instructions assume you are using a UNIX-like system such as a Linux distribution
or Mac OS X.

**Java**: You will need a Java 7+ runtime environment.  LinkBench by default
      uses the version of Java on your path.  You can override this by setting the
      JAVA\_HOME environment variable to the directory of the desired
      Java runtime version.  You will also need a Java JDK to compile from source.

**Maven**: To build LinkBench, you will need the Apache Maven build tool. If
    you do not have it already, it is available from http://maven.apache.org .

**MySQL Connector**:  To benchmark MySQL with LinkBench, you need MySQL
    Connector/J, A version of the MySQL connector is bundled with
    LinkBench.  If you wish to use a more recent version, replace the
    mysql jar under lib/.  See http://dev.mysql.com/downloads/connector/j/

**MySQL Server**: To benchmark MySQL you will need a running MySQL
    server with free disk space.

Getting and Building LinkBench
----------------------------
First get the source code

    git clone git@github.com:facebook/linkbench.git

Then enter the directory and build LinkBench

    cd linkbench
    mvn clean package

In order to skip slower tests (some run quite long), type

    mvn clean package -P fast-test

To skip all tests

    mvn clean package -DskipTests

If the build is successful, you should get a message like this at the end of the output:

    BUILD SUCCESSFUL
    Total time: 3 seconds

If the build fails while downloading required files, you may need to configure Maven,
for example to use a proxy.  Example Maven proxy configuration is shown here:
http://maven.apache.org/guides/mini/guide-proxies.html

Now you can run the LinkBench command line tool:

    ./bin/linkbench

Running it without arguments will show a brief help message:

    Did not select benchmark mode
    usage: linkbench [-c <file>] [-csvstats <file>] [-csvstream <file>] [-D
           <property=value>] [-L <file>] [-l] [-r]
     -c <file>                       Linkbench config file
     -csvstats,--csvstats <file>     CSV stats output
     -csvstream,--csvstream <file>   CSV streaming stats output
     -D <property=value>             Override a config setting
     -L <file>                       Log to this file
     -l                              Execute loading stage of benchmark
     -r                              Execute request stage of benchmark

Running a Benchmark with MySQL
==============================
In this section we will document the process of setting
up a new MySQL database and running a benchmark with LinkBench.

MySQL Setup
-----------
We need to create a new database and tables on the MySQL server.
We'll create a new database called `linkdb` and
the needed tables to store graph nodes, links and link counts.
Run the following commands in the MySQL console:

    create database linkdb;
    use linkdb;

    CREATE TABLE `linktable` (
      `id1` bigint(20) unsigned NOT NULL DEFAULT '0',
      `id2` bigint(20) unsigned NOT NULL DEFAULT '0',
      `link_type` bigint(20) unsigned NOT NULL DEFAULT '0',
      `visibility` tinyint(3) NOT NULL DEFAULT '0',
      `data` varchar(255) NOT NULL DEFAULT '',
      `time` bigint(20) unsigned NOT NULL DEFAULT '0',
      `version` int(11) unsigned NOT NULL DEFAULT '0',
      PRIMARY KEY (link_type, `id1`,`id2`),
      KEY `id1_type` (`id1`,`link_type`,`visibility`,`time`,`id2`,`version`,`data`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1 PARTITION BY key(id1) PARTITIONS 16;

    CREATE TABLE `counttable` (
      `id` bigint(20) unsigned NOT NULL DEFAULT '0',
      `link_type` bigint(20) unsigned NOT NULL DEFAULT '0',
      `count` int(10) unsigned NOT NULL DEFAULT '0',
      `time` bigint(20) unsigned NOT NULL DEFAULT '0',
      `version` bigint(20) unsigned NOT NULL DEFAULT '0',
      PRIMARY KEY (`id`,`link_type`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

    CREATE TABLE `nodetable` (
      `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
      `type` int(10) unsigned NOT NULL,
      `version` bigint(20) unsigned NOT NULL,
      `time` int(10) unsigned NOT NULL,
      `data` mediumtext NOT NULL,
      PRIMARY KEY(`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

You may want to set up a special database user account for benchmarking:

    -- Note: replace 'linkbench'@'localhost' with 'linkbench'@'%' to allow remote connections
    CREATE USER 'linkbench'@'localhost' IDENTIFIED BY 'mypassword';
    -- Grant all privileges on linkdb to this user
    GRANT ALL ON linkdb TO 'linkbench'@'localhost'

If you want to obtain representative benchmark results, we highly
recommend that you invest some time configuring and tuning MySQL.
MySQL performance tuning can be complex and a comprehensive guide
is beyond the scope of this readme, but here are a few basic guidelines:
* Read the [Optimization section of the MySQL user manual](http://dev.mysql.com/doc/refman/5.6/en/optimization.html).
* Make sure you have a sensible size setting for the [InnoDB buffer pool size](http://dev.mysql.com/doc/refman/5.6/en/optimizing-innodb-diskio.html),
  so as to reduce disk I/O.
* Table partitioning (as shown above) can eliminate some bottlenecks
  that occur with LinkBench where the linktable is heavily accessed.

Configuration Files
-------------------
LinkBench requires several configuration files that specify the
benchmark setup, the parameters of the graph to be generated, etc.
Before benchmarking you will want to make a copy of the example config file:

    cp config/LinkConfigMysql.properties config/MyConfig.properties

Open MyConfig.properties.  At a minimum you will need to fill in the
settings under *MySQL Connection Information* to match the server, user
and database you set up earlier. E.g.

    # MySQL connection information
    host = localhost
    user = linkbench
    password = your_password
    port = 3306
    dbid = linkdb

You can read through the settings in this file.  There are a lot of settings
that control the benchmark itself, and the output of the LinkBench
command link tool.  Notice that MyConfig.properties
references another file in this line:

    workload_file = config/FBWorkload.properties

This workload file defines how the social
graph should be generated and what mix of operations should make
up the benchmark.  The included workload file has been tuned to
match our production workload in query mix.  If you want to change
the scale of the benchmark (the default graph is quite small for
benchmarking purposes), you should look at the maxid1 setting.  This
controls the number of nodes in the initial graph created in the load
phase: increase it to get a larger database.

      # start node id (inclusive)
      startid1 = 1

      # end node id for initial load (exclusive)
      # With default config and MySQL/InnoDB, 1M ids ~= 1GB
      maxid1 = 10000001

Loading Data
------------
First we need to do an initial load of data using our new config file:

    ./bin/linkbench -c config/MyConfig.properties -l

This will take a while to load, and you should get frequent progress updates.
Once loading is finished you should see a notification like:

    LOAD PHASE COMPLETED.  Loaded 10000000 nodes (Expected 10000000).
      Loaded 47423071 links (4.74 links per node).  Took 620.4 seconds.
      Links/second = 76435

At the end LinkBench reports a range of statistics on load time that are
of limited interest at this stage.


You can significantly speed up the LinkBench load phase by making these
temporary changes in the MySQL command shell before loading:

    alter table linktable drop key `id1_type`;
    set global innodb_flush_log_at_trx_commit = 2;
    set global sync_binlog = 0;

After loading you should revert the changes:

    set global innodb_flush_log_at_trx_commit = 1;
    set global sync_binlog = 1;
    alter table linktable add key `id1_type`
      (`id1`,`link_type`,`visibility`,`time`,`id2`,`version`,`data`);

Request Phase
-------------
Now you can do some benchmarking.
Run the request phase using the below command:

    ./bin/linkbench -c config/MyConfig.properties -r

LinkBench will log progress to the console, along with statistics.
Once all requests have been sent, or the time limit has elapsed, LinkBench
will notify you of completion:

    REQUEST PHASE COMPLETED. 25000000 requests done in 2266 seconds.
      Requests/second = 11029

You can also inspect the latency statistics. For example, the following line tells us the mean latency
for link range scan operations, along with latency ranges for median (p50), 99th percentile (p99) and
so on.

    GET_LINKS_LIST count = 12678653  p25 = [0.7,0.8]ms  p50 = [1,2]ms
                   p75 = [1,2]ms  p95 = [10,11]ms  p99 = [15,16]ms
                   max = 2064.476ms  mean = 2.427ms

Advanced LinkBench Command Line Usage
-------------------------------------
Here are some further examples of how to use the LinkBench command link utility.

You can override any properties from the configuration file from the
command line with -D key=value.  For example, this runs the benchmark
with a 10 minute warmup before collecting statistics:

    ./bin/linkbench -c config/MyConfig.properties -D warmup_time=600 -r

This runs the benchmark with more detailed logging, and all output going to the file linkbench.log:

    ./bin/linkbench -c config/MyConfig.properties -D debuglevel=DEBUG -L linkbench.log -r

LinkBench supports output of statistics in csv format for easier analysis.
There are two categories of statistic: the final summary and per-thread statistics
output periodically through the benchmark.  -csvstats controls the former and -csvstream the latter:

    ./bin/linkbench -c config/MyConfig.properties -csvstats final-stats.csv -csvstreams streaming-stats.csv -r


Benchmark Guidelines
====================
Benchmarks are often controversial and are challenging to do well.
Here are some guidelines for avoiding common pitfalls with LinkBench.

Database Tuning
---------------
To remove confounding factors in database setup, there are several steps you can take
to obtain better results:

* Warm up the databases before collecting statistics.  LinkBench has a
  *warmup_time* setting that sends requests for a period before starting to
  collect statistics.
* Run benchmarks for long periods of time (hours rather than minutes)
    to reduce impact of random variation and to allow the database to
    reach a steady state.
* If at all possible, get expert help tuning the database for your
  hardware and workload.
* Benchmarks where the database fits mostly or entirely in RAM are interesting
  but aren't comparable to benchmarks where
  the database is much larger than RAM.  Typically for MySQL benchmarks our databases
  are 10-15x larger than the buffer pool.
* Databases should be benchmarked in comparable configurations.  We
  always run LinkBench with durable writes (i.e. so that
  after an operation returns, the data is written to persistent storage and can be
  recovered in the event of a system crash).
  Similarly, our LinkBench MySQL implementation provides serializable consistency of
  operations.  Weaker durability or consistency properties should be
  disclosed alongside benchmark results.

Understanding Performance Profile Under Varying Load
----------------------------------------------------
Different systems can behave different when heavily or lightly loaded.
The default benchmark settings simulate a heavily loaded database,
with 100 concurrent request threads each sending requests as quickly as they can.
Some database systems perform better than others with many concurrent
clients or heavy load, so performance under heavy load does not
give a complete picture of performance.
Typically databases are not fully loaded all of the time,
so latency of requests under moderate load is also an important
measure of database performance.

To get a better understanding of database performance under varying load it
can be helpful to:
* Modify the *requesters* parameter to test database performance with varying
  numbers of clients.
* Modify the *requestrate* config setting so that requests are throttled.
  Request latency vs. throughput curves help with understanding the full
  performance profile of a database system.

Understanding Resource Utilization
-------------------------
If you are doing a benchmark exercise, it is often a good idea to collect
additional information about system resource utilization, particularly
for CPU and I/O.  This can aid a lot in understanding and comparing
benchmark results beyond headline performance numbers.  It is
easiest to make use of collected data if you can match up timestamps
to your benchmark logs, so the examples here will append
timestamps to each line of output.

vmstat reports useful summary information on CPU and memory:

    vmstat 1 | gawk '{now=strftime("%Y-%m-%d %T "); print now $0}' > linkbench.run.1/vmstat.out

iostat reports some useful I/O statistics:

    iostat -d -x 1 |  gawk '{now=strftime("%Y-%m-%d %T "); print now $0}' > linkbench.run.1/iostat.out


Extending/Customizing LinkBench
===============================
You can customize LinkBench in several ways.

Reconfiguring Workload
---------------------
We have already introduced you to the LinkBench configuration files.
All settings in these files are documented and a great deal can be changed
simply through these configuration files.  For example:

* You can experiment with read-intensive or write-intensive workloads by
  modifying the mix of operations.
* You can alter the mix of hot/cold rows by modifying the shape
  parameter for ZipfDistribution.  If you set it close to 1, there will be only
  a few very hot nodes in the database, or if you set if close to 0, accesses
  will be spread evenly across all nodes.

Additional Workload Generators
------------------------------
It is possible to further customize the data and workload by providing
new implementations of some key classes:
* ProbabilityDistribution: which can be used to control the distribution of
    out-edges in the graph, or the access patterns for requests.
* DataGenerator: which can be used to generate data in different ways for requests.

Additional Database Systems
---------------------------
You can write plugins to benchmark additional database systems
simply by writing a Java class implementing a small set of graph operations.
Any classes implementing the `com.facebook.LinkBench.LinkStore`
and `com.facebook.LinkBench.NodeStore` interfaces can be loaded
through the *linkstore* and *nodestore* configuration file keys.

There are several steps you will have to go through to add a
new plugin .
First you need to choose you will represent LinkBench
nodes and links.  Several factors play a role in the design, but
speed of range scans and atomicity of updates are particularly important.
The MySQL schema from earlier in this README serves as a reference
implementation.

Next you need to create a new Java class, such as `public class MyStore
extends GraphStore`, and implement all of the required methods of
`LinkStore` and `NodeStore`.  Two reference implementations are provided:
`LinkStoreMysql`, a fully-fledged implementation,  and `MemoryLinkStore`,
a toy in-memory implementation.

LinkBench provides some tests to validate your implementation that you
can use during development.  If you extend any of the test classes
`LinkStoreTestBase`, `NodeStoreTestBase` and `GraphStoreTestBase` with
the required methods that set up your database, then a range of tests
will be run against it.  These tests are sanity checks rather than
comprehensive verification of your implementation.  In particular,
they do not try to verify the atomicity, consistency or durability
properties of the implementation.

Database-specific tests are not run by default.  You can enable them
with Maven profiles.  For example, to run the MySQL tests you can
run:

    mvn test -P mysql-test

The MySQL related unit tests are run against a test database that needs
setting up before running the unit tests. The default settings for this
test database are hardcoded in src/test/java/com/facebook/LinkBench/MySqlTestConfig.java.
The default settings uses localhost:3306 to connect to the database and
uses username "linkbench" and password "linkbench".  The unit test code 
creates all the required tables, so the developer needs to setup a 
MySql database called "linkbench_unittestdb" to which the linkbench user 
has permissions to create and drop tables.

**If you implement a plugin for a new database, please consider contributing
it back to the main LinkBench distribution with a pull request.**
