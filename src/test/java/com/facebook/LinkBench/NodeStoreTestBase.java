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
import java.util.Arrays;
import java.util.Properties;

import junit.framework.TestCase;

import org.junit.Test;

/**
 * This test implements unit tests that *all* implementations of NodeStore
 * should pass.
 *
 * Different implementations of NodeStore will require different configuration
 * and different setups for testing, so in order to test out a particular
 * NodeStore implementation, you can subclass this test and implement the
 * required abstract methods so that the test store is initialized correctly
 * and all required configuration properties are filled in.
 *
 * @author tarmstrong
 */
public abstract class NodeStoreTestBase extends TestCase {

  protected String testDB = "linkbench_unittestdb";

  protected abstract void initNodeStore(Properties props)
          throws Exception, IOException;

  protected abstract NodeStore getNodeStoreHandle(boolean initialized)
          throws Exception, IOException;

  protected Properties basicProps() {
    Properties props = new Properties();
    props.setProperty(Config.DBID, testDB);
    return props;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Properties props = basicProps();

    // Set up db
    initNodeStore(props);
    getNodeStoreHandle(true).resetNodeStore(testDB, 0);
  }

  @Test
  public void testIDAlloc() throws IOException, Exception {
    int now = (int)(System.currentTimeMillis()/1000L);
    NodeStore store = getNodeStoreHandle(true);

    final int nodeType = 2048;
    final long initId = 4; // We always start counting from 4 at Facebook
    store.resetNodeStore(testDB, initId); // Start from clean store

    byte data[] = new byte[] {0xb, 0xe, 0xa, 0x5, 0x7};
    Node test = new Node(-1, nodeType, 1, now, data);

    long id = store.addNode(testDB, test);
    // Check id allocated
    assertEquals("expected first ID allocated after reset", initId, id);
    assertEquals("addNode should not modify arguments", -1, test.id);
    test.id = id;

    // Allocate another
    id = store.addNode(testDB, test);
    test.id = id;
    long secondId = initId + 1;
    assertEquals("expected second ID allocated after reset", secondId, id);

    // Check retrieval
    Node fetched = store.getNode(testDB, nodeType, secondId);
    assertNotSame("Fetched nodes should not alias", fetched, test);
    assertEquals("Check fetched node" + fetched + ".equals(" + test + ")",
               test, fetched); // but should have same data

    // Check deletion
    assertTrue(store.deleteNode(testDB, nodeType, secondId));
    assertNull(store.getNode(testDB, nodeType, secondId));
    // Delete non-existent data
    assertFalse("Deleting non-existent node should fail",
            store.deleteNode(testDB, nodeType, 8));
    int otherType = nodeType + 1;
    assertFalse("Node should not be deleted if types don't match",
            store.deleteNode(testDB, otherType, initId));

    // Check reset works right
    long newInitId = initId - 1;
    store.resetNodeStore(testDB, newInitId);
    assertNull("Nodes should be deleted after reset",
                  store.getNode(testDB, nodeType, newInitId));
    assertEquals("Correct ID after second reset", newInitId,
                  store.addNode(testDB, test));
    assertEquals("Correct ID after second reset", newInitId + 1,
                  store.addNode(testDB, test));
    assertNotNull("Added node should exist",
                  store.getNode(testDB, nodeType, newInitId));
    assertNotNull("Added node should exist",
                  store.getNode(testDB, nodeType, newInitId + 1));
  }

  @Test
  public void testUpdate() throws IOException, Exception {
    NodeStore store = getNodeStoreHandle(true);
    store.resetNodeStore(testDB, 0);

    Node test = new Node(-1, 1234, 3, 3, "the quick brown fox".getBytes());
    test.id = store.addNode(testDB, test);
    test.data = "jumped over the lazy dog".getBytes();
    assertTrue(store.updateNode(testDB, test));

    Node test2 = store.getNode(testDB, test.type, test.id);
    assertNotNull(test2);
    assertTrue(test.equals(test2));
  }

  @Test
  public void testBinary() throws IOException, Exception {
    byte data[] = new byte[4096];

    for (int i = 0; i < data.length; i++) {
      data[i] = (byte)(i % 256);
    }

    NodeStore store = getNodeStoreHandle(true);
    store.resetNodeStore(testDB, 0);

    Node test = new Node(-1, 1234, 3, 3, data);
    test.id = store.addNode(testDB, test);

    Node test2 = store.getNode(testDB, test.type, test.id);
    assertNotNull(test2);
    assertTrue(Arrays.equals(data, test2.data));

    byte data2[] = new byte[data.length * 2];
    for (int i = 0; i < data2.length; i++) {
      data2[i] = (byte)((i + 52) % 256);
    }
    test.data = data2;
    assertTrue(store.updateNode(testDB, test));

    Node test3 = store.getNode(testDB, test.type, test.id);
    assertNotNull(test3);
    assertTrue(Arrays.equals(data2, test3.data));
  }

}
