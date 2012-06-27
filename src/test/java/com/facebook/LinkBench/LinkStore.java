package com.facebook.LinkBench;

import java.io.*;
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
  public static final int ADD_LINK = 0;
  public static final int DELETE_LINK = 1;
  public static final int UPDATE_LINK = 2;
  public static final int COUNT_LINK = 3;
  public static final int GET_LINK = 4;
  public static final int GET_LINKS_LIST = 5;
  public static final int LOAD_LINK = 6;
  public static final int UNKNOWN = 7;

  // Although the following is not an operation type, we need stats on
  // how big range scans are
  public static final int RANGE_SIZE = 8;

  public static final int MAX_OPTYPES = 9;

  public static final String displaynames[] =
  {
      "ADD_LINK",
      "DELETE_LINK",
      "UPDATE_LINK",
      "COUNT_LINK",
      "GET_LINK",
      "GET_LINKS_LIST",
      "LOAD_LINK",
      "UNKNOWN",
      "RANGE_SIZE",
  };

  /** The default constructor */
  public LinkStore() {
  }

  /** initialize the store object */
  public abstract void initialize(Properties p,
    int currentPhase, int threadId) throws IOException, Exception;

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
}
