/*
 * Copyright 2012, Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.LinkBench;

import com.facebook.LinkBench.store.mysql.LinkStoreMysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Class containing hardcoded parameters and helper functions used to create
 * and connect to the unit test database for MySql
 * @author tarmstrong
 */
public class MySqlTestConfig {

  // Hardcoded parameters for now
  static String host = "localhost";
  static int port = 3306;
  static String user = "linkbench";
  static String pass = "linkbench";
  static String linktable = "test_linktable";
  static String counttable = "test_counttable";
  static String nodetable = "test_nodetable";

  public static void fillMySqlTestServerProps(Properties props) {
    props.setProperty(Config.LINKSTORE_FACTORY_CLASS, LinkStoreMysql.class.getName());
    props.setProperty(Config.NODESTORE_FACTORY_CLASS, LinkStoreMysql.class.getName());
    props.setProperty(LinkStoreMysql.CONFIG_HOST, host);
    props.setProperty(LinkStoreMysql.CONFIG_PORT, Integer.toString(port));
    props.setProperty(LinkStoreMysql.CONFIG_USER, user);
    props.setProperty(LinkStoreMysql.CONFIG_PASSWORD, pass);
    props.setProperty(Config.LINK_TABLE, linktable);
    props.setProperty(Config.COUNT_TABLE, counttable);
    props.setProperty(Config.NODE_TABLE, nodetable);
  }

  static Connection createConnection(String testDB)
     throws InstantiationException,
      IllegalAccessException, ClassNotFoundException, SQLException {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    return DriverManager.getConnection(
            "jdbc:mysql://"+ MySqlTestConfig.host + ":" +
                    MySqlTestConfig.port + "/" + testDB +
            "?elideSetAutoCommits=true" +
            "&useLocalTransactionState=true" +
            "&allowMultiQueries=true" +
            "&useLocalSessionState=true",
            MySqlTestConfig.user, MySqlTestConfig.pass);
  }


  static void createTestTables(Connection conn, String testDB)
                                          throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(String.format(
        "CREATE TABLE `%s`.`%s` (" +
        "`id1` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`id2` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`link_type` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`visibility` tinyint(3) NOT NULL DEFAULT '0'," +
        "`data` varchar(255) NOT NULL DEFAULT ''," +
        "`time` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`version` int(11) unsigned NOT NULL DEFAULT '0'," +
        "PRIMARY KEY (`id1`,`id2`,`link_type`)," +
        "KEY `id1_type` (`id1`,`link_type`,`visibility`,`time`,`version`,`data`)" +
        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;",
        testDB, MySqlTestConfig.linktable));
    stmt.executeUpdate(String.format("CREATE TABLE `%s`.`%s` (" +
        "`id` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`link_type` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`count` int(10) unsigned NOT NULL DEFAULT '0'," +
        "`time` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`version` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "PRIMARY KEY (`id`,`link_type`)" +
        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;",
        testDB, MySqlTestConfig.counttable));
    stmt.executeUpdate(String.format(
        "CREATE TABLE `%s`.`%s` (" +
        "`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT," +
        "`type` int(10) unsigned NOT NULL," +
        "`version` bigint(20) unsigned NOT NULL," +
        "`time` int(10) unsigned NOT NULL," +
        "`data` mediumtext NOT NULL," +
        "primary key(`id`)" +
        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;",
        testDB, MySqlTestConfig.nodetable));
  }

  static void dropTestTables(Connection conn, String testDB)
                                                    throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`;",
        testDB, MySqlTestConfig.linktable));
    stmt.executeUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`;",
        testDB, MySqlTestConfig.counttable));
    stmt.executeUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`;",
        testDB, MySqlTestConfig.nodetable));
  }
}
