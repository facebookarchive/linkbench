package com.facebook.LinkBench;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.LinkCount;
import com.facebook.LinkBench.LinkStore;
import com.facebook.LinkBench.Phase;

/**
 * Can either be used as a wrapper around an existing LinkStore instance that
 * logs operations, or as a dummy linkstore instance that does nothing
 *
 */
public class DummyLinkStore extends LinkStore {

  public LinkStore wrappedStore;
  
  public DummyLinkStore() {
    this(null);
  }
  
  public DummyLinkStore(LinkStore wrappedStore) {
    this(wrappedStore, false);
  }
  
  public DummyLinkStore(LinkStore wrappedStore, boolean alreadyInitialized) {
    this.wrappedStore = wrappedStore;
    this.initialized = alreadyInitialized;
  }
  
  /**
   * @return true if real data is written and can be queried
   */
  public boolean isRealStore() {
    return wrappedStore != null;
  }
  
  public boolean initialized = false;
  
  public long adds = 0;
  public long deletes = 0;
  public long updates = 0;
  public long getLinks = 0;
  public long getLinkLists = 0;
  public long getLinkListsHistory = 0;
  public long countLinks = 0;

  public int bulkLoadBatchSize;
  public long bulkLoadLinkOps;
  public long bulkLoadLinkRows;
  public long bulkLoadCountOps;
  public long bulkLoadCountRows;
  
  @Override
  public void initialize(Properties p, Phase currentPhase, int threadId)
      throws IOException, Exception {
    if (initialized) {
      throw new Exception("Double initialization");
    }
    initialized = true;
    if (wrappedStore != null) {
      wrappedStore.initialize(p, currentPhase, threadId);
    }
  }

  @Override
  public void close() {
    checkInitialized();
    initialized = false;
    if (wrappedStore != null) {
      wrappedStore.close();
    }
  }

  @Override
  public void clearErrors(int threadID) {
    checkInitialized();
    if (wrappedStore != null) {
      wrappedStore.clearErrors(threadID);
    }
  }

  @Override
  public void addLink(String dbid, Link a, boolean noinverse) throws Exception {
    checkInitialized();
    adds++;
    if (wrappedStore != null) {
      wrappedStore.addLink(dbid, a, noinverse);
    }
  }

  @Override
  public void deleteLink(String dbid, long id1, long link_type, long id2,
      boolean noinverse, boolean expunge) throws Exception {
    checkInitialized();
    deletes++;
    // TODO Auto-generated method stub

    if (wrappedStore != null) {
      wrappedStore.deleteLink(dbid, id1, link_type, id2, noinverse, expunge);
    }
  }

  @Override
  public void updateLink(String dbid, Link a, boolean noinverse)
      throws Exception {
    checkInitialized();
    updates++;
    // TODO Auto-generated method stub

    if (wrappedStore != null) {
      wrappedStore.updateLink(dbid, a, noinverse);
    }
  }

  @Override
  public Link getLink(String dbid, long id1, long link_type, long id2)
      throws Exception {
    checkInitialized();
    getLinks++;
    // TODO Auto-generated method stub
    if (wrappedStore != null) {
      return wrappedStore.getLink(dbid, id1, link_type, id2);
    } else {
      return null;
    }
  }

  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type)
      throws Exception {
    checkInitialized();
    getLinkLists++;
    // TODO Auto-generated method stub
    if (wrappedStore != null) {
      return wrappedStore.getLinkList(dbid, id1, link_type);
    } else {
      return null;
    }
  }

  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type,
      long minTimestamp, long maxTimestamp, int offset, int limit)
      throws Exception {
    checkInitialized();
    getLinkLists++;
    getLinkListsHistory++;
    if (wrappedStore != null) {
      return wrappedStore.getLinkList(dbid, id1, link_type, minTimestamp,
                                      maxTimestamp, offset, limit);
    } else {
      return null;
    }
  }

  @Override
  public long countLinks(String dbid, long id1, long link_type)
      throws Exception {
    checkInitialized();
    countLinks++;
    // TODO Auto-generated method stub
    if (wrappedStore != null) {
      return wrappedStore.countLinks(dbid, id1, link_type);
    } else {
      return 0;
    }
  }

  private void checkInitialized() {
    if (!initialized) {
      throw new RuntimeException("Expected store to be initialized");
    }
  }

  @Override
  public int bulkLoadBatchSize() {
    if (wrappedStore != null) {
      return wrappedStore.bulkLoadBatchSize();
    } else{
      return bulkLoadBatchSize;
    }
  }

  @Override
  public void addBulkLinks(String dbid, List<Link> a, boolean noinverse)
      throws Exception {
    bulkLoadLinkOps++;
    bulkLoadLinkRows += a.size();
    if (wrappedStore != null) {
      wrappedStore.addBulkLinks(dbid, a, noinverse);
    }
  }

  @Override
  public void addBulkCounts(String dbid, List<LinkCount> a) throws Exception {
    bulkLoadCountOps++;
    bulkLoadCountRows += a.size();
    if (wrappedStore != null) {
      wrappedStore.addBulkCounts(dbid, a);
    }
  }

  @Override
  public int getRangeLimit() {
    if (wrappedStore != null) {
      return wrappedStore.getRangeLimit();
    } else {
      return rangeLimit;
    }
  }

  @Override
  public void setRangeLimit(int rangeLimit) {
    if (wrappedStore != null) {
      wrappedStore.setRangeLimit(rangeLimit);
    } else {
      this.rangeLimit = rangeLimit;
    }
  }
}
