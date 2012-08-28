package com.facebook.LinkBench;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.facebook.LinkBench.LinkBenchLoad.LoadChunk;
import com.facebook.LinkBench.LinkBenchLoad.LoadProgress;
import com.facebook.LinkBench.LinkBenchRequest.RequestProgress;
import com.facebook.LinkBench.distributions.UniformDistribution;
import com.facebook.LinkBench.distributions.AccessDistributions.AccessDistMode;
import com.facebook.LinkBench.distributions.LinkDistributions.LinkDistMode;

/**
 * This test implements unit tests that *all* implementations of LinkStore 
 * should pass.
 * 
 * Different implementations of LinkStore will require different configuration
 * and different setups for testing, so in order to test out a particular 
 * LinkStore implementation, you can subclass this test and implement the
 * required abstract methods so that the test store is initialized correctly
 * and all required configuration properties are filled in.
 * 
 * @author tarmstrong
 */
public abstract class LinkStoreTestBase extends TestCase {

  protected String testDB = "linkbench_unittestdb";
  private Logger logger = Logger.getLogger("");

  /**
   * Reinitialize link store database properties.
   * Should attempt to clean database
   * @param props Properties for test DB.
   *        Override any required properties in this property dict
   */
  protected abstract void initStore(Properties props)
                                    throws IOException, Exception;
  
  /**
   * Override to vary size of test
   * @return number of ids to use in testing
   */
  protected long getIDCount() {
    return 50000;
  }
  
  /**
   * Override to vary number of requests in test
   */
  protected int getRequestCount() {
    return 100000;
  }
  
  /**
   * Override to vary maximum number of threads
   */
  protected int maxConcurrentThreads() {
    return Integer.MAX_VALUE;
  }
  
  /** Get a new handle to the initialized store, wrapped in
   * DummyLinkStore
   * @return new handle to linkstore
   */
  protected abstract DummyLinkStore getStoreHandle() 
                                    throws IOException, Exception;
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    initStore(basicProps());
  }
  
  /**
   * Provide properties for basic test store
   * @return
   */
  protected Properties basicProps() {
    Properties props = new Properties();
    props.setProperty(Config.DBID, testDB);
    return props;
  }
  
  private void fillLoadProps(Properties props, long startId, long idCount,
      int linksPerId) {
    props.setProperty(Config.MIN_ID,Long.toString(startId));
    props.setProperty(Config.MAX_ID, Long.toString(startId + idCount));
    props.setProperty(Config.RANDOM_ID2_MAX, "0");
    props.setProperty(Config.LINK_DATASIZE, "100");
    // Fixed number of rows
    props.setProperty(Config.NLINKS_FUNC, LinkDistMode.CONST.name()); 
    props.setProperty(Config.NLINKS_CONFIG, "0"); // ignored
    props.setProperty(Config.NLINKS_DEFAULT, Integer.toString(linksPerId));
    props.setProperty(Config.DISPLAY_FREQ, "1800");
    props.setProperty(Config.MAX_STAT_SAMPLES, "10000");
  }

  private void fillReqProps(Properties props, long startId, long idCount,
      int requests, long timeLimit, double p_addlink, double p_deletelink,
      double p_updatelink, double p_countlink, double p_getlink,
      double p_getlinklist) {
    props.setProperty(Config.MIN_ID,Long.toString(startId));
    props.setProperty(Config.MAX_ID, Long.toString(startId + idCount));
    props.setProperty(Config.NUM_REQUESTS, Long.toString(requests));
    props.setProperty(Config.MAX_TIME, Long.toString(timeLimit));
    
    props.setProperty(Config.RANDOM_ID2_MAX, "0");
    props.setProperty(Config.ID2GEN_CONFIG, "0");
    
    props.setProperty(Config.PR_ADD_LINK, Double.toString(p_addlink));
    props.setProperty(Config.PR_DELETE_LINK, Double.toString(p_deletelink));
    props.setProperty(Config.PR_UPDATE_LINK, Double.toString(p_updatelink));
    props.setProperty(Config.PR_COUNT_LINKS, Double.toString(p_countlink));
    props.setProperty(Config.PR_GET_LINK, Double.toString(p_getlink));
    props.setProperty(Config.PR_GET_LINK_LIST, Double.toString(p_getlinklist));
    
    props.setProperty(Config.WRITE_FUNCTION,
                                        UniformDistribution.class.getName());
    props.setProperty(Config.READ_FUNCTION, AccessDistMode.RECIPROCAL.name());
    props.setProperty(Config.READ_CONFIG, "0");
  }

  /** 
   * Utility to create a random number generator and print
   * the seed for later reproducibility of test failures
   * @return
   */
  private Random createRNG() {
    long randSeed = System.currentTimeMillis();
    System.out.println("Random seed: " + randSeed);
    Random rng = new Random(randSeed);
    return rng;
  }

  /** Simple test with multiple operations on single link */
  @Test
  public void testOneLink() throws IOException, Exception {
    DummyLinkStore store = getStoreHandle();

    long id1 = 1123, id2 = 1124, ltype = 321;
    Link writtenLink = new Link(id1, ltype, id2, 1, 1, 
        LinkStore.VISIBILITY_DEFAULT, new byte[] {0x1}, 1, 1994);
    store.addLink(testDB, writtenLink, true);
    if (store.isRealStore()) {
      Link readBack = store.getLink(testDB, id1, ltype, id2);
      assertNotNull(readBack);
      if (!writtenLink.equals(readBack)) {
        throw new Exception("Expected " + readBack.toString() + " to equal "
            + writtenLink.toString());
      }
      assertEquals(1, store.countLinks(testDB, id1, ltype));
    }
    
    // Try expunge
    store.deleteLink(testDB, id1, ltype, id2, true, true);
    assertNull(store.getLink(testDB, id1, ltype, id2));
    assertNull(store.getLinkList(testDB, id1, ltype));
    assertEquals(0, store.countLinks(testDB, id1, ltype));
    
    store.addLink(testDB, writtenLink, true);
    if (store.isRealStore()) {
      assertNotNull(store.getLink(testDB, id1, ltype, id2));
      assertEquals(1, store.countLinks(testDB, id1, ltype));
    }
    // try hiding
    store.deleteLink(testDB, id1, ltype, id2, true, false);
    if (store.isRealStore()) {
      Link hidden = store.getLink(testDB, id1, ltype, id2);
      assertNotNull(hidden);
      assertEquals(LinkStore.VISIBILITY_HIDDEN, hidden.visibility);
      // Check it is same up to visibility
      Link check = hidden.clone();
      check.visibility = LinkStore.VISIBILITY_DEFAULT;
      assertTrue(writtenLink.equals(check));
      assertEquals(0, store.countLinks(testDB, id1, ltype));
      assertNull(store.getLinkList(testDB, id1, ltype));
    }
    
    // Update link: check it is unhidden
    store.updateLink(testDB, writtenLink, true);
    if (store.isRealStore()) {
      assertTrue(writtenLink.equals(store.getLink(testDB, id1, ltype, id2)));
      assertEquals(1, store.countLinks(testDB, id1, ltype));
      Link links[] = store.getLinkList(testDB, id1, ltype);
      assertEquals(1, links.length);
      assertTrue(writtenLink.equals(links[0]));
    }
    
    store.deleteLink(testDB, id1, ltype, id2, true, true);
  }
  
  @Test
  public void testMultipleLinks() throws Exception, IOException {
    DummyLinkStore store = getStoreHandle();
    long ida = 5434, idb = 5435, idc = 9999, idd = 9998;
    long ltypea = 1, ltypeb = 2;
    int otype = 35342; 
    
    byte data[] = new byte[] {0xf, 0xa, 0xc, 0xe, 0xb, 0x0, 0x0, 0xc};
    long t = 10000000;
    Link links[] = new Link[] {
       new Link(ida, ltypea, idc, otype, otype, LinkStore.VISIBILITY_DEFAULT,
           data, 1, System.currentTimeMillis()),
       new Link(ida, ltypeb, idc, otype, otype, LinkStore.VISIBILITY_DEFAULT,
           data, 1, System.currentTimeMillis()),
       new Link(idb, ltypeb, ida, otype, otype, LinkStore.VISIBILITY_DEFAULT,
           data, 1,  t + 1),
       new Link(idb, ltypeb, idb, otype, otype, LinkStore.VISIBILITY_DEFAULT,
           data, 1, t),
       new Link(idb, ltypeb, idc, otype, otype, LinkStore.VISIBILITY_HIDDEN,
           data, 1, t - 2),
       new Link(idb, ltypeb, idd, otype, otype, LinkStore.VISIBILITY_DEFAULT,
           data, 1, t + 3),
    };
    for (Link l: links) {
      store.addLink(testDB, l, true);
    }
    if (store.isRealStore()) {
      // Check counts
      assertEquals(1, store.countLinks(testDB, ida, ltypea));
      assertEquals(1, store.countLinks(testDB, ida, ltypeb));
      assertEquals(0, store.countLinks(testDB, idb, ltypea));
      assertEquals(3, store.countLinks(testDB, idb, ltypeb));
      
      Link retrieved[];
      
      retrieved = store.getLinkList(testDB, ida, ltypea);
      assertEquals(1, retrieved.length);
      assertTrue(links[0].equals(retrieved[0]));
      
      retrieved = store.getLinkList(testDB, ida, ltypeb);
      assertEquals(1, retrieved.length);
      assertTrue(links[1].equals(retrieved[0]));
      
      retrieved = store.getLinkList(testDB, idb, ltypeb);
      // Check link list, Four matching links, one hidden
      checkExpectedList(store, idb, ltypeb, links[5], links[2], links[3]);
      
      // Check limit
      retrieved = store.getLinkList(testDB, idb, ltypeb, 
          0, t + 100, 0, 1);
      assertEquals(1, retrieved.length);
      assertTrue(links[5].equals(retrieved[0]));
      
      //Check offset + limit
      retrieved = store.getLinkList(testDB, idb, ltypeb, 
          0, t + 100, 1, 2);
      assertEquals(2, retrieved.length);
      assertTrue(links[2].equals(retrieved[0]));
      assertTrue(links[3].equals(retrieved[1]));
      
      // Check range filtering
      retrieved = store.getLinkList(testDB, idb, ltypeb, 
          t + 1, t + 2, 0, Integer.MAX_VALUE);
      assertEquals(1, retrieved.length);
      assertTrue(links[2].equals(retrieved[0]));
    }
  }

  /**
   * Regression test for flaw in MySql where visibility is assumed to
   * be default on add
   */
  @Test
  public void testHiding() throws Exception {
    DummyLinkStore store = getStoreHandle();
    Link l = new Link(1, 1, 1, 1, 1, 
          LinkStore.VISIBILITY_HIDDEN, new byte[] {0x1}, 1,
          System.currentTimeMillis());
    store.addLink(testDB, l, true);
    checkExpectedList(store, 1, 1, new Link[0]);
    
    // Check that updating works right
    store.deleteLink(testDB, 1, 1, 1, true, false);
    checkExpectedList(store, 1, 1, new Link[0]);
    
    // Make it visible
    l.visibility = LinkStore.VISIBILITY_DEFAULT;
    store.addLink(testDB, l, true);
    checkExpectedList(store, 1, 1, l);
    
    // Expunge
    store.deleteLink(testDB, 1, 1, 1, true, true);
    checkExpectedList(store, 1, 1, new Link[0]);
  }
  
  /**
   * Regression test for bad handling of string escaping
   */
  @Test
  public void testSqlInjection() throws IOException, Exception {
    Link l = new Link(1, 1, 1, 1, 1, LinkStore.VISIBILITY_DEFAULT, 
                "' asdfasdf".getBytes(), 1, 1);
    byte updateData[] = "';\\".getBytes();
    
    testAddThenUpdate(l, updateData);
  }

  private void testAddThenUpdate(Link l, byte[] updateData) throws IOException,
      Exception {
    DummyLinkStore ls = getStoreHandle();
    ls.addLink(testDB, l, true);
    
    Link l2 = ls.getLink(testDB, 1, 1, 1);
    if (ls.isRealStore()) {
      assertNotNull(l2);
      assertTrue(l.equals(l2));
    }
    
    l.data = updateData;
    ls.updateLink(testDB, l, true);
    l2 = ls.getLink(testDB, 1, 1, 1);
    if (ls.isRealStore()) {
      assertNotNull(l2);
      assertTrue(l.equals(l2));
    }
  }
  
  /** Check handling of bytes 0-127 */
  @Test
  public void testBinary1() throws IOException, Exception {
    binaryDataTest(0, 128);
  }
  
  /** Check handling of bytes 160-256 */
  @Test
  public void testBinary2() throws IOException, Exception {
    int start = 160;
    binaryDataTest(start, 256-start);
  }
  
  /** Check handling of bytes 128-159 */
  @Test
  public void testBinary3() throws IOException, Exception {
    int start = 128;
    binaryDataTest(start, 159-start);
  }

  /**
   * Test insertion/update of binary data: insert binary string with
   * bytes [startByte:startByte + dataMaxSize) and read back
   * @throws IOException
   * @throws Exception
   */
  private void binaryDataTest(int startByte, int dataMaxSize)
      throws IOException, Exception {
    byte data[] = new byte[dataMaxSize];
    for (int i = 0; i < data.length; i++) {
      byte b = (byte)((i + startByte) % 256);
      data[i] = b;
    }
    Link l = new Link(1, 1, 1, 1, 1, LinkStore.VISIBILITY_DEFAULT, 
                data, 1, 1);
    // Different length and data
    byte updateData[] = new byte[dataMaxSize/2];
    for (int i = 0; i < updateData.length; i++) {
      updateData[i] = (byte)((i + startByte ) % 256);
    }
    testAddThenUpdate(l, updateData);
  }
  
  
  
  /**
   * Generic test for a loader using a wrapped LinkStore
   * implementation
   * @throws Exception 
   * @throws IOException 
   */
  @Test
  public void testLoader() throws IOException, Exception {
    long startId = 0;
    long idCount = getIDCount();
    int linksPerId = 3;
    long testStartTime = System.currentTimeMillis();

    Properties props = basicProps();
    fillLoadProps(props, startId, idCount, linksPerId);
    
    initStore(props);
    DummyLinkStore store = getStoreHandle();
    
    try {
      Random rng = createRNG();
      
      serialLoad(rng, logger, props, store);
      
      long testEndTime = System.currentTimeMillis();
      
      assertFalse(store.initialized); // Check was closed
      
      /* Validate results */
      if (store.bulkLoadBatchSize() > 0) {
        assertEquals(idCount, store.bulkLoadCountRows);
      }
      assertEquals(idCount * linksPerId, store.bulkLoadLinkRows + store.adds);
      
      if (store.isRealStore()) {
        // old store was closed by loader
        store.initialize(props, Phase.REQUEST, 0);
        // read back data and sanity check
        validateLoadedData(logger, store, startId, idCount, linksPerId,
            testStartTime, testEndTime);
      }
    } finally {
      if (!store.initialized) {
        store.initialize(props, Phase.REQUEST, 0);
      }
      deleteIDRange(store, startId, idCount);
    }
  }

  /**
   * Run the requester against 
   * This test validates both the requester (by looking at counts to make
   * sure it at least did the right number of ops) and the LinkStore
   * (by stress-testing it).
   * @throws Exception 
   * @throws IOException 
   */
  @Test
  public void testRequester() throws IOException, Exception {
    long startId = 532;
    long idCount = getIDCount();
    int linksPerId = 5;
    
    int requests = getRequestCount();
    long timeLimit = requests;


    Properties props = basicProps();
    fillLoadProps(props, startId, idCount, linksPerId);
    
    double p_add = 0.2, p_del = 0.2, p_up = 0.1, p_count = 0.1, 
           p_get = 0.2, p_getlinks = 0.2;
    fillReqProps(props, startId, idCount, requests, timeLimit,
        p_add * 100, p_del * 100, p_up * 100, p_count * 100, p_get * 100,
        p_getlinks * 100);
    
    try {
      Random rng = createRNG();
      
      serialLoad(rng, logger, props, getStoreHandle());
  
      DummyLinkStore reqStore = getStoreHandle();
      LinkBenchLatency latencyStats = new LinkBenchLatency(1);
      RequestProgress tracker = new RequestProgress(logger, requests, timeLimit);
      
      LinkBenchRequest requester = new LinkBenchRequest(reqStore,
                      null, props, latencyStats, tracker, rng, 0, 1);
      
      requester.run();
      
      assertTrue(reqStore.adds + reqStore.updates + reqStore.deletes +
          reqStore.countLinks + reqStore.getLinks + reqStore.getLinkLists
          == requests);
      // Check that the proportion of operations is roughly right - within 1%
      // For now, updates are actually implemented as add operations
      assertTrue(Math.abs(reqStore.adds / (double)requests - 
          (p_add + p_up)) < 0.01);
      assertTrue(Math.abs(reqStore.updates / 
                      (double)requests - 0.0) < 0.01);
      assertTrue(Math.abs(reqStore.deletes /
                       (double)requests - p_del) < 0.01);
      assertTrue(Math.abs(reqStore.countLinks / 
                       (double)requests - p_count) < 0.01);
      assertTrue(Math.abs(reqStore.getLinks /
                       (double)requests - p_get) < 0.01);
      assertTrue(Math.abs(reqStore.getLinkLists / 
                       (double)requests - p_getlinks) < 0.01);
      assertEquals(0, reqStore.bulkLoadCountOps);
      assertEquals(0, reqStore.bulkLoadLinkOps);
    } finally {
      deleteIDRange(getStoreHandle(), startId, idCount);
    }
    System.err.println("Done!");
  }
  
  /**
   * Test that the requester throttling slows down requests
   * @throws Exception 
   * @throws IOException 
   */
  @Test
  public void testRequesterThrottling() throws IOException, Exception {
    long startId = 1000000;
    // Small test
    long idCount = getIDCount() / 10;
    int linksPerId = 3;
    
    Properties props = basicProps();
    int requests = 2000;
    long timeLimit = requests;
    int requestsPerSec = 500; // Limit to fairly low rate
    fillLoadProps(props, startId, idCount, linksPerId);
    fillReqProps(props, startId, idCount, requests, timeLimit,
                 20, 20, 10, 10, 20, 20);
    props.setProperty("requestrate", Integer.toString(requestsPerSec));
    
    try {
      Random rng = createRNG();
      
      serialLoad(rng, logger, props, getStoreHandle());
      RequestProgress tracker = new RequestProgress(logger, requests, timeLimit);
      
      DummyLinkStore reqStore = getStoreHandle();
      LinkBenchRequest requester = new LinkBenchRequest(reqStore, null,
                      props, new LinkBenchLatency(1), tracker, rng, 0, 1);
      
      long startTime = System.currentTimeMillis();
      requester.run();
      long endTime = System.currentTimeMillis();
      
      assertEquals(requests, reqStore.adds + reqStore.updates + reqStore.deletes +
          reqStore.countLinks + reqStore.getLinks + reqStore.getLinkLists);
      double actualArrivalRate = 1000 * requests / (double)(endTime - startTime);
      System.err.println("Expected request rate: " + requestsPerSec
          + " actual request rate: " + actualArrivalRate);
      // Check that it isn't more that 5% faster than expected average
      assertTrue(actualArrivalRate <= 1.05 * requestsPerSec);
    } finally {
      deleteIDRange(getStoreHandle(), startId, idCount);
    }
    System.err.println("Done!");
  }

  private void checkExpectedList(DummyLinkStore store,
            long id1, long ltype, Link... expected) throws Exception {
    if (!store.isRealStore()) return;
    assertEquals(expected.length, store.countLinks(testDB, id1, ltype));
    Link actual[] = store.getLinkList(testDB, id1, ltype);
    if (expected.length == 0) {
      assertNull(actual);
    } else {
      assertEquals(expected.length, actual.length);
      for (int i = 0; i < expected.length; i++) {
        if (!expected[i].equals(actual[i])) {
          fail("Mismatch between result lists. Expected: " +
        		Arrays.toString(expected) + " Actual: " + Arrays.toString(actual));
        }
      }
    }
  }
  
  /**
   * Use the LinkBenchLoad class to do a serial load of data
   * @param logger
   * @param props
   * @param store
   * @param idCount
   * @throws IOException
   * @throws Exception
   */
  private void serialLoad(Random rng, Logger logger, Properties props,
      DummyLinkStore store) throws IOException, Exception {
    LinkBenchLatency latencyStats = new LinkBenchLatency(1);
    
    /* Load up queue with work */
    BlockingQueue<LoadChunk>  chunk_q = new LinkedBlockingQueue<LoadChunk>();
    long startId = Long.parseLong(props.getProperty(Config.MIN_ID));
    long idCount = Long.parseLong(props.getProperty(Config.MAX_ID)) - startId;
    
    int chunkSize = 128;
    int seq = 0;
    for (long i = startId; i < startId + idCount; i+= chunkSize) {
      LoadChunk chunk = new LoadChunk(seq, i, 
                        Math.min(idCount + startId, i + chunkSize), rng);
      chunk_q.add(chunk);
      seq++;
    }
    chunk_q.add(LoadChunk.SHUTDOWN);
    
    
    LoadProgress tracker = new LoadProgress(logger, idCount);
    tracker.startTimer();
    LinkBenchLoad loader = new LinkBenchLoad(store, 
        props, latencyStats, 0, false, chunk_q, tracker);
    /* Run the loading process */
    loader.run();
    
    logger.info("Loaded " + (store.adds + store.bulkLoadLinkRows) + " links. "
        + store.adds + " individually " + " and " + store.bulkLoadLinkRows
        + " in rows");
  }

  private void validateLoadedData(Logger logger, DummyLinkStore wrappedStore,
      long startId, long idCount, int linksPerId, long minTimestamp,
      long maxTimestamp) throws Exception {
    for (long i = startId; i < startId + idCount; i++) {
      assertEquals(wrappedStore.countLinks(testDB, i, LinkStore.LINK_TYPE),
                   linksPerId);
      
      Link links[] = wrappedStore.getLinkList(testDB, i, LinkStore.LINK_TYPE);
      if (linksPerId == 0) {
        assertTrue(links == null);
      } else {
        assertEquals(links.length, linksPerId);
        long lastTimestamp = Long.MAX_VALUE;
        for (Link l: links) {
          assertEquals(l.id1, i);
          assertEquals(l.link_type, LinkStore.LINK_TYPE);
          assertEquals(l.visibility, LinkStore.VISIBILITY_DEFAULT);
          assertEquals(l.id1_type, LinkStore.ID1_TYPE);
          assertEquals(l.id2_type, LinkStore.ID2_TYPE);
          // Check timestamp in correct range
          assertTrue(l.time >= minTimestamp);
          assertTrue(l.time <= maxTimestamp);
          // Check descending
          assertTrue(lastTimestamp >= l.time);
          lastTimestamp = l.time;
        }
      }
    }
    logger.info("Successfully sanity checked data for " + idCount + " ids");
  }

  private void deleteIDRange(DummyLinkStore store, long startId, long idCount)
      throws Exception {
    // attempt to delete data
    for (long i = startId; i < startId + idCount; i++) {
      Link links[] = store.getLinkList(testDB, i, LinkStore.LINK_TYPE);
      if (links != null) {
        for (Link l: links) {
          assert(l != null);
          store.deleteLink(testDB, l.id1, l.link_type, l.id2,
                                  true, true);
        }
      }
    }
  }
}
