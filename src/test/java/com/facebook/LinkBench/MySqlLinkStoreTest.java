package com.facebook.LinkBench;

import java.io.IOException;
import java.sql.Connection;
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
    MySqlTestConfig.dropTestTables(conn, testDB);
    MySqlTestConfig.createTestTables(conn, testDB);
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
    MySqlTestConfig.dropTestTables(conn, testDB);
    conn.close();
  }

}
