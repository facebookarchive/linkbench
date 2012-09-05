package com.facebook.LinkBench;

import java.io.IOException;
import java.sql.Connection;
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
    MySqlTestConfig.dropTestTables(conn, testDB);
    MySqlTestConfig.createTestTables(conn, testDB);
  }

  @Override
  protected NodeStore getNodeStoreHandle(boolean initialize) throws Exception, IOException {
    DummyLinkStore result = new DummyLinkStore(new LinkStoreMysql());
    if (initialize) {
      result.initialize(currProps, Phase.REQUEST, 0);
    }
    return result;
  }

}
