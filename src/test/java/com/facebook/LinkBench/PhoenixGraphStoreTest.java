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

@Category(PhoenixTest.class)
public class PhoenixGraphStoreTest extends GraphStoreTestBase {

  private Properties props;
  private Connection conn;

  @Override
  protected void initStore(Properties props) throws IOException, Exception {
    this.props = props;
    this.conn = PhoenixTestConfig.createConnection(testDB);
    PhoenixTestConfig.dropTestTables(conn, testDB);
    PhoenixTestConfig.createTestTables(conn, testDB);
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
    return 2000;
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    PhoenixTestConfig.dropTestTables(conn, testDB);
  }

  @Override
  protected Properties basicProps() {
    Properties props = super.basicProps();
    PhoenixTestConfig.fillPhoenixTestServerProps(props);
    return props;
  }


  @Override
  protected DummyLinkStore getStoreHandle(boolean initialize) throws IOException, Exception {
    DummyLinkStore result = new DummyLinkStore(new LinkStorePhoenix());
    if (initialize) {
      result.initialize(props, Phase.REQUEST, 0);
    }
    return result;
  }

}
