package com.facebook.LinkBench;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.LinkBenchLatency;
import com.facebook.LinkBench.LinkBenchLoad;
import com.facebook.LinkBench.LinkStore;
import com.facebook.LinkBench.Phase;
import com.facebook.LinkBench.LinkBenchLoad.LoadChunk;
import com.facebook.LinkBench.LinkBenchLoad.LoadProgress;

public abstract class LinkStoreTestBase extends TestCase {

  protected String testDB = "linkbench_unittestdb";

  /**
   * @param props Properties for test DB.
   *        Override any required properties in this property dict
   * @return new instance of a linkstore, or null just for dummy linkstore
   */
  protected abstract LinkStore createStore(Properties props)
                                    throws IOException, Exception;
  
  /**
   * Override to vary size of test
   * @return number of ids to use in testing
   */
  protected long getIDCount() {
    return 50000;
  }
  
  protected DummyLinkStore createWrappedStore(Properties props) 
                                    throws IOException, Exception {
    return new DummyLinkStore(createStore(props));
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

    Properties props = new Properties();
    props.setProperty("startid1",Long.toString(startId));
    props.setProperty("maxid1", Long.toString(startId + idCount + 1));
    props.setProperty("randomid2max", "0");
    props.setProperty("datasize", "100");
    props.setProperty("nlinks_func", "-3"); // Fixed number of rows
    props.setProperty("nlinks_config", "0"); // ignored
    props.setProperty("nlinks_default", Integer.toString(linksPerId));
    props.setProperty("displayfreq", "1800");
    props.setProperty("maxsamples", "10000");
    props.setProperty("dbid", testDB);
    
    DummyLinkStore store = createWrappedStore(props);
    Logger logger = Logger.getLogger("");
    
    try {
      serialLoad(logger, props, store, startId, idCount);
      
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

  /**
   * Use the LinkBenchLoad class to do a serial load of data
   * @param logger
   * @param props
   * @param store
   * @param idCount
   * @throws IOException
   * @throws Exception
   */
  private void serialLoad(Logger logger, Properties props, DummyLinkStore store,
      long startId, long idCount) throws IOException, Exception {
    store.initialize(props, Phase.LOAD, 0);
    LinkBenchLatency latencyStats = new LinkBenchLatency(1);
    
    /* Load up queue with work */
    BlockingQueue<LoadChunk>  chunk_q = new LinkedBlockingQueue<LoadChunk>();
    int chunkSize = 128;
    int seq = 0;
    for (long i = startId; i < startId + idCount; i+= chunkSize) {
      LoadChunk chunk = new LoadChunk(seq, i, Math.min(idCount, i + chunkSize));
      chunk_q.add(chunk);
      seq++;
    }
    chunk_q.add(LoadChunk.SHUTDOWN);
    
    
    LoadProgress tracker = new LoadProgress(logger, idCount);
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
}
