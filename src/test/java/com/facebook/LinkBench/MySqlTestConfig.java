package com.facebook.LinkBench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
    props.setProperty(Config.LINKSTORE_CLASS, LinkStoreMysql.class.getName());
    props.setProperty(Config.NODESTORE_CLASS, LinkStoreMysql.class.getName());
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
}
