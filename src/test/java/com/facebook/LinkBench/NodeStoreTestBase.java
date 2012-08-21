package com.facebook.LinkBench;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.Test;

public abstract class NodeStoreTestBase extends TestCase {

  protected String testDB = "linkbench_unittestdb";
  private Logger logger = Logger.getLogger("");
  
  protected abstract void initNodeStore(Properties props)
          throws Exception, IOException;
  protected abstract NodeStore getNodeStoreHandle() 
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
    getNodeStoreHandle().resetNodeStore(testDB, 0);
  }
  
  @Test
  public void testIDAlloc() throws IOException, Exception {
    int now = (int)(System.currentTimeMillis()/1000L);
    NodeStore store = getNodeStoreHandle();
    
    Node test = new Node(-1, 2048, 1, now,  new byte[] {0xb, 0xe, 0xa, 0x5, 0x7});
    store.resetNodeStore(testDB, 4); // We always start counting from 4 at Facebook
    long id = store.addNode(testDB, test);
    // Check id allocated
    assertEquals(4, test.id);
    assertEquals(4, id);
    
    // Allocate another
    id = store.addNode(testDB, test);
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
    NodeStore store = getNodeStoreHandle();
    store.resetNodeStore(testDB, 0);
    
    Node test = new Node(-1, 1234, 3, 3, "the quick brown fox".getBytes());
    store.addNode(testDB, test);
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
    
    NodeStore store = getNodeStoreHandle();
    store.resetNodeStore(testDB, 0);
    
    Node test = new Node(-1, 1234, 3, 3, data);
    store.addNode(testDB, test);
    
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
