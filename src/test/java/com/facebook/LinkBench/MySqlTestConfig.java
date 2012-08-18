package com.facebook.LinkBench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

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
    props.setProperty("store", "com.facebook.LinkBench.LinkStoreMysql");
    props.setProperty("host", host);
    props.setProperty("port", Integer.toString(port));
    props.setProperty("user", user);
    props.setProperty("password", pass);
    props.setProperty("tablename", linktable);
    props.setProperty("counttable", counttable);
    props.setProperty("nodetable", nodetable);
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
