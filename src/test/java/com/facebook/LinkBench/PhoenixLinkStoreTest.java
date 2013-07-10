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

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

import org.junit.experimental.categories.Category;

import com.facebook.LinkBench.testtypes.PhoenixTest;

/**
 * Test the Phoenix LinkStore implementation.
 *
 */
@Category(PhoenixTest.class)
public class PhoenixLinkStoreTest extends LinkStoreTestBase {

  private Connection conn;

  /** Properties for last initStore call */
  private Properties currProps;

  @Override
  protected long getIDCount() {
    // Make test smaller so that it doesn't take too long
    return 1000;
  }

  @Override
  protected int getRequestCount() {
    // Fewer requests to keep test quick
    return 2000;
  }

  protected Properties basicProps() {
    Properties props = super.basicProps();
    PhoenixTestConfig.fillPhoenixTestServerProps(props);
    return props;
  }


  @Override
  protected void initStore(Properties props) throws IOException, Exception {
    this.currProps = (Properties)props.clone();
    if (conn != null) {
      conn.close();
    }
    conn = PhoenixTestConfig.createConnection(testDB);
    PhoenixTestConfig.dropTestTables(conn, testDB);
    PhoenixTestConfig.createTestTables(conn, testDB);
  }



  @Override
  public DummyLinkStore getStoreHandle(boolean initialize) throws IOException, Exception {
    DummyLinkStore result = new DummyLinkStore(new LinkStorePhoenix());
    if (initialize) {
      result.initialize(currProps, Phase.REQUEST, 0);
    }
    return result;
  }

  @Override protected void tearDown() throws Exception {
    super.tearDown();
    PhoenixTestConfig.dropTestTables(conn, testDB);
    conn.close();
  }

}
