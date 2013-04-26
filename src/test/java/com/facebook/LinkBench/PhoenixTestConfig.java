/*
 * Copyright 2012, Facebook, Inc.
 * Copyright 2013, Intel, Inc.
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Class containing hard coded parameters and helper functions used to create
 * and connect to the unit test database for Phoenix.
 * @author Raymond Liu
 */
public class PhoenixTestConfig {

  // Hardcoded parameters for now
  static String host = "localhost";
  static int port = 2181;
  static String user = "linkbench";
  static String pass = "linkbench";
  static String linktable = "test_linktable";
  static String counttable = "test_counttable";
  static String nodetable = "test_nodetable";

  public static void fillPhoenixTestServerProps(Properties props) {
    props.setProperty(Config.LINKSTORE_CLASS, LinkStorePhoenix.class.getName());
    props.setProperty(Config.NODESTORE_CLASS, LinkStorePhoenix.class.getName());
    props.setProperty(LinkStorePhoenix.CONFIG_HOST, host);
    props.setProperty(LinkStorePhoenix.CONFIG_PORT, Integer.toString(port));
    props.setProperty(LinkStorePhoenix.CONFIG_USER, user);
    props.setProperty(LinkStorePhoenix.CONFIG_PASSWORD, pass);
    props.setProperty(Config.LINK_TABLE, linktable);
    props.setProperty(Config.COUNT_TABLE, counttable);
    props.setProperty(Config.NODE_TABLE, nodetable);
  }

  static Connection createConnection(String testDB)
     throws InstantiationException,
      IllegalAccessException, ClassNotFoundException, SQLException {
    
    String jdbcUrl = "jdbc:phoenix:"+ host + ":" + port;
    Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver").newInstance();
    return DriverManager.getConnection(jdbcUrl);
  }


  static void createTestTables(Connection conn, String testDB)
                                          throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(String.format(
        "CREATE TABLE %s (" +
        "id1 BIGINT NOT NULL," +
        "id2 BIGINT NOT NULL," +
        "link_type BIGINT NOT NULL," +
        "visibility INTEGER NOT NULL," +
        "data VARBINARY(255) NOT NULL," +
        "time BIGINT NOT NULL," +
        "version INTEGER NOT NULL " +
        "CONSTRAINT pk PRIMARY KEY (id1, id2, link_type)" +
        ")",
        PhoenixTestConfig.linktable));
    stmt.executeUpdate(String.format("CREATE TABLE %s (" +
        "id BIGINT NOT NULL," +
        "link_type BIGINT NOT NULL," +
        "count BIGINT NOT NULL," +
        "time BIGINT NOT NULL," +
        "version BIGINT NOT NULL " +
        "CONSTRAINT pk PRIMARY KEY (id, link_type)" +
        ")",
        PhoenixTestConfig.counttable));
    stmt.executeUpdate(String.format(
        "CREATE TABLE %s (" +
        "id BIGINT NOT NULL PRIMARY KEY," +
        "type INTEGER NOT NULL," +
        "version BIGINT NOT NULL," +
        "time INTEGER NOT NULL," +
        "data VARBINARY NOT NULL" +
        ")",
        PhoenixTestConfig.nodetable));
  }

  static void dropTestTables(Connection conn, String testDB)
                                                    throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(String.format("DROP TABLE IF EXISTS %s",
        PhoenixTestConfig.linktable));
    stmt.executeUpdate(String.format("DROP TABLE IF EXISTS %s",
        PhoenixTestConfig.counttable));
    stmt.executeUpdate(String.format("DROP TABLE IF EXISTS %s",
        PhoenixTestConfig.nodetable));
  }
}
