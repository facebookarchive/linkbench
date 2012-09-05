package com.facebook.LinkBench;

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

public class MySqlGraphStoreTest extends GraphStoreTestBase {

  private Properties props;
  private Connection conn;
  
  @Override
  protected void initStore(Properties props) throws IOException, Exception {
    this.props = props;
    this.conn = MySqlTestConfig.createConnection(testDB);
    MySqlTestConfig.dropTestTables(conn, testDB);
    MySqlTestConfig.createTestTables(conn, testDB);
  }

  @Override
  protected long getIDCount() {
    // Make quicker
    return 500;
  }

  @Override
  protected int getRequestCount() {
    // Make quicker, enough requests that we can reasonably check
    // that operation percentages are about about right
    return 10000;
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    MySqlTestConfig.dropTestTables(conn, testDB);
  }

  @Override
  protected Properties basicProps() {
    Properties props = super.basicProps();
    MySqlTestConfig.fillMySqlTestServerProps(props);
    return props;
  }


  @Override
  protected DummyLinkStore getStoreHandle(boolean initialize) throws IOException, Exception {
    DummyLinkStore result = new DummyLinkStore(new LinkStoreMysql());
    if (initialize) {
      result.initialize(props, Phase.REQUEST, 0);
    }
    return result;
  }

}
