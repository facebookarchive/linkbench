/*
 * Copyright 2012, Facebook, Inc.
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

import com.facebook.LinkBench.testtypes.MySqlTest;

@Category(MySqlTest.class)
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
