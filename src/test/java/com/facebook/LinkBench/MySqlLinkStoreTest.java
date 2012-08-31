package com.facebook.LinkBench;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Test the MySQL LinkStore implementation.
 * 
 * Assumes that the database specified by the testDB field has been created
 * with permissions for a user/pass linkbench/linkbench to create tables, select,
 * insert, delete, etc.
 */
public class MySqlLinkStoreTest extends LinkStoreTestBase {
  
  private Connection conn;
  
  /** Properties for last initStore call */
  private Properties currProps;
  
  @Override
  protected long getIDCount() {
    // Make test smaller so that it doesn't take too long
    return 5000;
  }

  @Override
  protected int getRequestCount() {
    // Fewer requests to keep test quick
    return 10000;
  }
  
  protected Properties basicProps() {
    Properties props = super.basicProps();
    MySqlTestConfig.fillMySqlTestServerProps(props);
    return props;
  }


  @Override
  protected void initStore(Properties props) throws IOException, Exception {
    this.currProps = (Properties)props.clone();
    if (conn != null) {
      conn.close();
    }
    conn = MySqlTestConfig.createConnection(testDB);
    dropTestTables();
    createTestTables();
  }

  

  @Override
  public DummyLinkStore getStoreHandle(boolean initialize) throws IOException, Exception {
    DummyLinkStore result = new DummyLinkStore(new LinkStoreMysql());
    if (initialize) {
      result.initialize(currProps, Phase.REQUEST, 0);
    }
    return result;
  }

  @Override protected void tearDown() throws Exception {
    super.tearDown();
    dropTestTables();
    conn.close();
  }

  private void createTestTables() throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(String.format(
        "CREATE TABLE `%s`.`%s` (" + 
        "`id1` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`id1_type` int(10) unsigned NOT NULL DEFAULT '0'," +
        "`id2` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`id2_type` int(10) unsigned NOT NULL DEFAULT '0'," +
        "`link_type` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`visibility` tinyint(3) NOT NULL DEFAULT '0'," +
        "`data` varchar(255) NOT NULL DEFAULT ''," +
        "`time` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`version` int(11) unsigned NOT NULL DEFAULT '0'," +
        "PRIMARY KEY (`id1`,`id2`,`link_type`)," +
        "KEY `id2_vis` (`id2`,`visibility`)," +
        "KEY `id1_type` (`id1`,`link_type`,`visibility`,`time`,`version`,`data`)" +
        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;", 
        testDB, MySqlTestConfig.linktable));
    stmt.executeUpdate(String.format("CREATE TABLE `%s`.`%s` (" +
        "`id` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`id_type` int(10) unsigned NOT NULL DEFAULT '0'," +
        "`link_type` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`count` int(10) unsigned NOT NULL DEFAULT '0'," +
        "`time` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "`version` bigint(20) unsigned NOT NULL DEFAULT '0'," +
        "PRIMARY KEY (`id`,`link_type`)" +
        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;",
        testDB, MySqlTestConfig.counttable));
  }

  private void dropTestTables() throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`;",
        testDB, MySqlTestConfig.linktable));
    stmt.executeUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`;",
        testDB, MySqlTestConfig.counttable));
  }
}
