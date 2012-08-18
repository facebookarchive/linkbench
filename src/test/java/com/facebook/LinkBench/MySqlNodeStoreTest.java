package com.facebook.LinkBench;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class MySqlNodeStoreTest extends NodeStoreTestBase {

  Connection conn;
  Properties currProps;
  
  @Override
  protected Properties basicProps() {
    Properties props = super.basicProps();
    MySqlTestConfig.fillMySqlTestServerProps(props);
    return props;
  }

  @Override
  protected void initNodeStore(Properties props) throws Exception, IOException {
    currProps = props;
    conn = MySqlTestConfig.createConnection(testDB);
    dropTestTable();
    createTestTable();
  }

  private void dropTestTable() throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(String.format("DROP TABLE IF EXISTS `%s`.`%s`;",
        testDB, MySqlTestConfig.nodetable));
  }
  
  private void createTestTable() throws SQLException {
    Statement stmt = conn.createStatement();
    String sql = String.format(
        "CREATE TABLE `%s`.`%s` (" + 
        "`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT," +
        "`type` int(10) unsigned NOT NULL," +
        "`version` bigint(20) unsigned NOT NULL," +
        "`time` int(10) unsigned NOT NULL," +
        "`data` mediumtext NOT NULL," +
        "primary key(`id`)" +
        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;", 
        testDB, MySqlTestConfig.nodetable);
    stmt.executeUpdate(sql);
  }

  @Override
  protected NodeStore getNodeStoreHandle() throws Exception, IOException {
    LinkStoreMysql store = new LinkStoreMysql();
    store.initialize(currProps, Phase.REQUEST, 0);
    return store;
  }

}
