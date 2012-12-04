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
    
    Node test = new Node(-1, 2048, 1, now,  new byte[] {0xb, 0xe, 0xa, 0x5, 0x7});
    store.resetNodeStore(testDB, 4); // We always start counting from 4 at Facebook
    long id = store.addNode(testDB, test);
    // Check id allocated
    assertEquals(4, id);
    assertEquals(-1, test.id); // Check not modified
    test.id = id;
    
    // Allocate another
    id = store.addNode(testDB, test);
    test.id = id;
    assertEquals(5, id);
    
    // Check retrieval
    Node fetched = store.getNode(testDB, 2048, 5);
    assertTrue(fetched != test); // should not alias
    assertTrue(fetched + ".equals(" + test + ")",
               fetched.equals(test)); // but should have same data
    
    // Check deletion
    assertTrue(store.deleteNode(testDB, 2048, 5));
    assertNull(store.getNode(testDB, 2048, 5));
    // Delete non-existent data
    assertFalse(store.deleteNode(testDB, 2048, 8)); // bad id
    assertFalse(store.deleteNode(testDB, 2049, 4)); // bad type
    
    // Check reset works right
    store.resetNodeStore(testDB, 3);
    assertNull(store.getNode(testDB, 2048, 4));
    assertEquals(3, store.addNode(testDB, test));
    assertEquals(4, store.addNode(testDB, test));
    assertNotNull(store.getNode(testDB, 2048, 3));
    assertNotNull(store.getNode(testDB, 2048, 4));
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
