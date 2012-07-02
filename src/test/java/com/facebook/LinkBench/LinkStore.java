package com.facebook.LinkBench;

import java.io.*;
import java.util.List;
import java.util.Properties;

public abstract class LinkStore {
  // void createLinkTable();
  public static final long LINK_TYPE = 123456789;
  public static final long MAX_ID2 = Long.MAX_VALUE;
  public static final int ID1_TYPE = 2048;
  public static final int ID2_TYPE = 2048;

  // visibility
  public static final byte VISIBILITY_HIDDEN = 0;
  public static final byte VISIBILITY_DEFAULT = 1;
  
  // Various operation types for which we want to gather stats
  public static enum LinkStoreOp {
    ADD_LINK,
    DELETE_LINK,
    UPDATE_LINK,
    COUNT_LINK,
    GET_LINK,
    GET_LINKS_LIST,
    LOAD_LINK,
    LOAD_LINKS_BULK,
    LOAD_COUNTS_BULK,
    // Although the following are not truly operations, we need stats
    // for them 
    RANGE_SIZE,    // how big range scans are
    LOAD_LINKS_BULK_NLINKS, // how many links inserted in bulk
    LOAD_COUNTS_BULK_NLINKS, // how many counts inserted in bulk
    UNKNOWN,
  }

  public static final int MAX_OPTYPES = LinkStoreOp.values().length;

  
  public static final String displayName(LinkStoreOp op) {
    return op.name();
  }

  /** The default constructor */
  public LinkStore() {
  }

  /** initialize the store object */
  public abstract void initialize(Properties p,
      Phase currentPhase, int threadId) throws IOException, Exception;

  /**
   * Do any cleanup.  After this is called, store won't be reused
   */
  public abstract void close();

  // this is invoked when an error happens in case connection needs to be
  // cleaned up, reset, reopened, whatever 
  public abstract void clearErrors(int threadID);

  public abstract void addLink(String dbid, Link a, boolean noinverse) throws Exception;
  public abstract void deleteLink(String dbid, long id1, long link_type, long id2, 
                   boolean noinverse, boolean expunge) throws Exception;
  public abstract void updateLink(String dbid, Link a, boolean noinverse) 
    throws Exception;

  // lookup using id1, type, id2
  public abstract Link getLink(String dbid, long id1, long link_type, long id2) 
    throws Exception;

  // lookup using just id1, type
  public abstract Link[] getLinkList(String dbid, long id1, long link_type) 
    throws Exception;


  public abstract Link[] getLinkList(String dbid, long id1, long link_type, 
                            long minTimestamp, long maxTimestamp, 
                            int offset, int limit)
    throws Exception;

  // count the #links
  public abstract long countLinks(String dbid, long id1, long link_type) throws Exception;
  
  /** 
   * @return 0 if it doesn't support addBulkLinks and recalculateCounts methods
   *         If it does support them, return the maximum number of links that 
   *         can be added at a time */
  public int bulkLoadBatchSize() {
    return 0;
  }
  
  /** Add a batch of links without updating counts */
  public void addBulkLinks(String dbid, List<Link> a, boolean noinverse)
             throws Exception {
    throw new UnsupportedOperationException("addBulkLinks not supported for " +
    		"LinkStore subclass " + this.getClass().getName());
  }
  
  /** Add a batch of counts */
  public void addBulkCounts(String dbid, List<LinkCount> a)
      throws Exception {
    throw new UnsupportedOperationException("addBulkCounts not supported for " +
      "LinkStore subclass " + this.getClass().getName());
  }
}
