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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class LinkStorePhoenix extends GraphStore {

  /* MySql database server configuration keys */
  public static final String CONFIG_HOST = "host";
  public static final String CONFIG_PORT = "zk_port";
  public static final String CONFIG_USER = "user";
  public static final String CONFIG_PASSWORD = "password";
  public static final String CONFIG_BULK_INSERT_BATCH = "phoenix_bulk_insert_batch";

  public static final String DEFAULT_ZOOKEEPER_PORT = "2181"; 
  
  public static final int DEFAULT_BULKINSERT_SIZE = 1000;

  String linktable;
  String counttable;
  String nodetable;

  String host;
  String user;
  String pwd;
  String port;

  Level debuglevel;
  Connection conn;
  Statement stmt;

  private Phase phase;

  int bulkInsertSize = DEFAULT_BULKINSERT_SIZE;

  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
  private final Sequencer nodeIdGenerator = Sequencer.getSequencer("nodeID");
  
  // FIXME : This simple Sequencer can not go beyond process scope.
  // need to switch to real database sequencer when implemented by phoenix.
  private static class Sequencer {
    private String name;
    private AtomicLong value = new AtomicLong(0);
    private static Map<String, Sequencer> sequencerMap = new HashMap<String, Sequencer>();

    private Sequencer(String name) {
      this.name = name;
    }

    public synchronized static Sequencer getSequencer(String name) {
      Sequencer s = null; 
      if (sequencerMap.containsKey(name)) {
        s = sequencerMap.get(name);
      } else {
        s = new Sequencer(name);
        sequencerMap.put(name, s);
      }
      return s;
    }

    public String getName() {
      return name;
    }

    // return the current value then increase by 1
    public long next() {
      return value.getAndIncrement();
    }

    public void set(long v) {
      value.set(v);
    }
  }
  

  public LinkStorePhoenix() {
    super();
  }

  public LinkStorePhoenix(Properties props) throws IOException, Exception {
    super();
    initialize(props, Phase.LOAD, 0);
  }

  public void initialize(Properties props, Phase currentPhase,
    int threadId) throws IOException, Exception {
    counttable = ConfigUtil.getPropertyRequired(props, Config.COUNT_TABLE);
    if (counttable.equals("")) {
      String msg = "Error! " + Config.COUNT_TABLE + " is empty!"
          + "Please check configuration file.";
      logger.error(msg);
      throw new RuntimeException(msg);
    }

    nodetable = props.getProperty(Config.NODE_TABLE);
    if (nodetable.equals("")) {
      // For now, don't assume that node table is provided
      String msg = "Error! " + Config.NODE_TABLE + " is empty!"
          + "Please check configuration file.";
      logger.error(msg);
      throw new RuntimeException(msg);
    }

    host = ConfigUtil.getPropertyRequired(props, CONFIG_HOST);
    user = ConfigUtil.getPropertyRequired(props, CONFIG_USER);
    pwd = ConfigUtil.getPropertyRequired(props, CONFIG_PASSWORD);
    port = props.getProperty(CONFIG_PORT);

    if (port == null || port.equals("")) port = DEFAULT_ZOOKEEPER_PORT; //use default port
    debuglevel = ConfigUtil.getDebugLevel(props);
    phase = currentPhase;

    if (props.containsKey(CONFIG_BULK_INSERT_BATCH)) {
      bulkInsertSize = ConfigUtil.getInt(props, CONFIG_BULK_INSERT_BATCH);
    }

    // connect
    try {
      openConnection();
    } catch (Exception e) {
      logger.error("error connecting to database:", e);
      throw e;
    }

    linktable = ConfigUtil.getPropertyRequired(props, Config.LINK_TABLE);
  }

  // connects to test database
  private void openConnection() throws Exception {
    conn = null;
    stmt = null;

    String jdbcUrl = "jdbc:phoenix:"+ host + ":" + port;

    Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver").newInstance();
    conn = DriverManager.getConnection(jdbcUrl);
    //System.err.println("connected");
    conn.setAutoCommit(false);

    // TODO: phoenix only support ResultSet.TYPE_FORWARD_ONLY
    // Need to check whether it meets the requirements.

    //stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
    //                            ResultSet.CONCUR_READ_ONLY);

    stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public void close() {
    try {
      if (stmt != null) stmt.close();
      if (conn != null) conn.close();
    } catch (SQLException e) {
      logger.error("Error while closing Phoenix connection: ", e);
    }
  }

  public void clearErrors(int threadID) {
    logger.info("Reopening Phoenix connection in threadID " + threadID);

    try {
      if (conn != null) {
        conn.close();
      }

      openConnection();
    } catch (Throwable e) {
      e.printStackTrace();
      return;
    }
  }

  /**
   * Set of all JDBC SQLState strings that indicate a transient Phoenix error
   * that should be handled by retrying
   */
  private static final HashSet<String> retrySQLStates = populateRetrySQLStates();

  /**
   *  Populate retrySQLStates
   */
  private static HashSet<String> populateRetrySQLStates() {
    HashSet<String> states = new HashSet<String>();
    // FIXME: need to add something here?
    return states;
  }

  /**
   * Handle SQL exception by logging error and selecting how to respond
   * @param ex SQLException thrown by Phoenix JDBC driver
   * @return true if transaction should be retried
   */
  private boolean processSQLException(SQLException ex, String op) {
    boolean retry = retrySQLStates.contains(ex.getSQLState());
    String msg = "SQLException thrown by Phoenix driver during execution of " +
                 "operation: " + op + ".  ";
    msg += "Message was: '" + ex.getMessage() + "'.  ";
    msg += "SQLState was: " + ex.getSQLState() + ".  ";

    if (retry) {
      msg += "Error is probably transient, retrying operation.";
      logger.warn(msg);
    } else {
      msg += "Error is probably non-transient, will abort operation.";
      logger.error(msg);
    }
    return retry;
  }

  @Override
  public boolean addLink(String dbid, Link l, boolean noinverse)
    throws Exception {
    while (true) {
      try {
        return addLinkImpl(dbid, l, noinverse);
      } catch (SQLException ex) {
        if (!processSQLException(ex, "addLink")) {
          throw ex;
        }
      }
    }
  }

  private boolean addLinkImpl(String dbid, Link l, boolean noinverse)
      throws Exception {

    if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
      logger.debug("addLink " + l.id1 +
                         "." + l.id2 +
                         "." + l.link_type);
    }

    // FIXME: The way we query link then update count is not thread safe,
    // since we don't hold any lock between query and update.
    // Could lead to wrong result. Need a fix here.

    int update_count = 0;

    Link oldLink = getLink(dbid, l.id1, l.link_type, l.id2);

    if (oldLink == null) {
      if (l.visibility == VISIBILITY_DEFAULT) {
        // new link, and is visible.
        update_count = 1;
      }
    } else {
      if (oldLink.visibility != l.visibility) {
        // existing link, update count according to visibility.
        update_count = (l.visibility == VISIBILITY_DEFAULT) ? 1 : -1;
      }
    }

    int nrows = addLinksNoCount(dbid, Collections.singletonList(l));

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("nrows = " + nrows);
    }

    if (update_count != 0) {
      // We do update count by first insert time filed 
      // then update count and version.

      String statement = "UPSERT INTO " + counttable +
                      " (id, link_type, time) " +
                      "VALUES (" + l.id1 +
                      ", " + l.link_type +
                      ", " + l.time + ")";

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(statement);
      }
      stmt.executeUpdate(statement);
      
      statement = "UPSERT INTO " + counttable +
              " (id, link_type, count, version) " +
              "INCREASE VALUES (" + l.id1 +
              ", " + l.link_type +
              ", " + update_count +
              ", 1" + ")";
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace(statement);
      }

      stmt.executeUpdate(statement);
      conn.commit();
    }

    return oldLink != null;
  }

  /**
   * Internal method: add links without updating the count
   * @param dbid
   * @param links
   * @return
   * @throws Exception
   */
  private int addLinksNoCount(String dbid, List<Link> links)
      throws Exception {

    if (links.size() == 0)
      return 0;

    int nrows = 0;
    
    String query = "UPSERT INTO " + linktable + 
        " (id1, id2, link_type, visibility, data, time, version)" +
        " VALUES(?, ?, ?, ?, ?, ?, ?)";
    PreparedStatement statement = conn.prepareStatement(query);

    for (Link l : links) {
      statement.setLong(1, l.id1);
      statement.setLong(2, l.id2);
      statement.setLong(3, l.link_type);
      statement.setInt(4, l.visibility);
      statement.setBytes(5, l.data);
      statement.setLong(6, l.time);
      statement.setInt(7, l.version);
      statement.executeUpdate();
      
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(statement.toString());
      }
    }

    // FIXME : Will return accumulated uncommitted queries.
    // This might not be the right link number we actually updated.
    // Is there better way to do this?
    nrows = statement.getUpdateCount();
    conn.commit();
    
    return nrows;
}

  @Override
  public boolean deleteLink(String dbid, long id1, long link_type, long id2,
                         boolean noinverse, boolean expunge)
    throws Exception {
    while (true) {
      try {
        return deleteLinkImpl(dbid, id1, link_type, id2, noinverse, expunge);
      } catch (SQLException ex) {
        if (!processSQLException(ex, "deleteLink")) {
          throw ex;
        }
      }
    }
  }

  private boolean deleteLinkImpl(String dbid, long id1, long link_type, long id2,
      boolean noinverse, boolean expunge) throws Exception {
    if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
      logger.debug("deleteLink " + id1 +
                         "." + id2 +
                         "." + link_type);
    }

    // conn.setAutoCommit(false);

    // FIXME : query visibility then update count according, it is not thread safe. 

    // First do a select to check if the link is not there, is there and
    // hidden, or is there and visible;
    // Result could be either NULL, VISIBILITY_HIDDEN or VISIBILITY_DEFAULT.
    // In case of VISIBILITY_DEFAULT, later we need to mark the link as
    // hidden, and update counttable.
    String select = "SELECT visibility" +
                    " FROM " + linktable +
                    " WHERE id1 = " + id1 +
                    " AND id2 = " + id2 +
                    " AND link_type = " + link_type;

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(select);
    }

    ResultSet result = stmt.executeQuery(select);

    int visibility = -1;
    boolean found = false;
    int count_inc = 0;
    while (result.next()) {
      visibility = result.getInt("visibility");
      found = true;
    }

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(String.format("(%d, %d, %d) visibility = %d",
                id1, link_type, id2, visibility));
    }

    if (!found) {
      // do nothing
    } else if (visibility == VISIBILITY_HIDDEN && !expunge) {
      // do nothing
    } else {
      // either delete or mark the link as hidden
      String delete;

      if (!expunge) {
        delete = "UPSERT INTO " + linktable +
                 " (id1, id2, link_type, visibility)" +
                 " SELECT id1, id2, link_type, " + VISIBILITY_HIDDEN +
                 " FROM " + linktable +
                 " WHERE id1 = " + id1 +
                 " AND id2 = " + id2 +
                 " AND link_type = " + link_type;
      } else {
        delete = "DELETE FROM " + linktable +
                 " WHERE id1 = " + id1 +
                 " AND id2 = " + id2 +
                 " AND link_type = " + link_type;
      }

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(delete);
      }

      stmt.executeUpdate(delete);

      if (visibility == VISIBILITY_DEFAULT) {
        count_inc = -1;
      }
    }
    
    if (count_inc != 0) {
      // update count table
      long currentTime = (new Date()).getTime();

      String statement = "UPSERT INTO " + counttable
          + " (id, link_type, time) "
          + "VALUES (" + id1
          + ", " + link_type
          + ", " + currentTime + ")";

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(statement);
      }
      stmt.executeUpdate(statement);

      statement = "UPSERT INTO " + counttable
          + " (id, link_type, count, version) "
          + "INCREASE VALUES ("
          + id1 + ", "
          + link_type + ", "
          + count_inc
          + ", 1" + ")";

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(statement);
      }

      stmt.executeUpdate(statement);

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(statement);
      }
    }

    conn.commit();
    // conn.setAutoCommit(true);
    return found;
  }

  @Override
  public boolean updateLink(String dbid, Link l, boolean noinverse)
    throws Exception {
    // Retry logic is in addLink
    boolean added = addLink(dbid, l, noinverse);
    return !added; // return true if updated instead of added
  }


  // lookup using id1, type, id2
  @Override
  public Link getLink(String dbid, long id1, long link_type, long id2)
    throws Exception {
    while (true) {
      try {
        return getLinkImpl(dbid, id1, link_type, id2);
      } catch (SQLException ex) {
        if (!processSQLException(ex, "getLink")) {
          throw ex;
        }
      }
    }
  }

  private Link getLinkImpl(String dbid, long id1, long link_type, long id2)
    throws Exception {
    Link res[] = multigetLinks(dbid, id1, link_type, new long[] {id2});
    if (res == null) return null;
    assert(res.length <= 1);
    return res.length == 0 ? null : res[0];
  }


  @Override
  public Link[] multigetLinks(String dbid, long id1, long link_type,
                              long[] id2s) throws Exception {
    while (true) {
      try {
        return multigetLinksImpl(dbid, id1, link_type, id2s);
      } catch (SQLException ex) {
        if (!processSQLException(ex, "multigetLinks")) {
          throw ex;
        }
      }
    }
  }

  private Link[] multigetLinksImpl(String dbid, long id1, long link_type,
                                long[] id2s) throws Exception {
    StringBuilder querySB = new StringBuilder();
    querySB.append(" select id1, id2, link_type," +
        " visibility, data, time, " +
        " version from " + linktable +
        " where id1 = " + id1 + " and link_type = " + link_type +
        " and id2 in (");
    boolean first = true;
    for (long id2: id2s) {
      if (first) {
        first = false;
      } else {
        querySB.append(",");
      }
      querySB.append(id2);
    }

    querySB.append(")");
    String query = querySB.toString();

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("Query is " + query);
    }

    ResultSet rs = stmt.executeQuery(query);
    conn.commit();

    ArrayList<Link> links = new ArrayList<Link>();
    while (rs.next()) {
      Link l = createLinkFromRow(rs);
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("Lookup result: " + id1 + "," + link_type + "," +
                  l.id2 + " found");
      }
      links.add(l);
    }
    return links.toArray(new Link[links.size()]);
  }

  // lookup using just id1, type
  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type)
    throws Exception {
    // Retry logic in getLinkList
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
  }

  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type,
                            long minTimestamp, long maxTimestamp,
                            int offset, int limit)
    throws Exception {
    while (true) {
      try {
        return getLinkListImpl(dbid, id1, link_type, minTimestamp,
                               maxTimestamp, offset, limit);
      } catch (SQLException ex) {
        if (!processSQLException(ex, "getLinkListImpl")) {
          throw ex;
        }
      }
    }
  }

  private Link[] getLinkListImpl(String dbid, long id1, long link_type,
        long minTimestamp, long maxTimestamp,
        int offset, int limit)
            throws Exception {
    assert(offset >= 0);
    
    if (limit <=0) {
      limit = rangeLimit;
    }
    // TODO: Just fake the offset operation for Phoenix.
    // Need to check whether this can be implemented in Phoenix or not.

    // FIXME: Phoenix do not support very large limit, so this will not work correctly for
    // a large offset + limit.
    long querylimit = Math.min((long)(limit + offset), 10000);
    
    String query = " select id1, id2, link_type," +
                   " visibility, data, time," +
                   " version from " + linktable +
                   " where id1 = " + id1 + " and link_type = " + link_type +
                   " and time >= " + minTimestamp +
                   " and time <= " + maxTimestamp +
                   " and visibility = " + LinkStore.VISIBILITY_DEFAULT +
                   " order by time desc" +
                   " limit " + querylimit;

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("Query is " + query);
    }

    ResultSet rs = stmt.executeQuery(query);
    conn.commit();

    // Fetch the link data
    ArrayList<Link> links = new ArrayList<Link>();
    int index = 0;
    while (rs.next()) {
      // skip offset number of result.
      if (++index <= offset) {
        continue;
      }
      // only accept limit number of results.
      if (index > (querylimit)) {
        break;
      }
      Link l = createLinkFromRow(rs);
      links.add(l);
    }

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("Range lookup result: " + id1 + "," + link_type +
                         " is " + links.size());
    }

    int resultSize = links.size();
    return resultSize == 0 ? null : links.toArray(new Link[resultSize]);
  }

  private Link createLinkFromRow(ResultSet rs) throws SQLException {
    Link l = new Link();
    l.id1 = rs.getLong(1);
    l.id2 = rs.getLong(2);
    l.link_type = rs.getLong(3);
    // FIXME : Use getByte when phoenix support it.
    l.visibility = (byte) rs.getInt(4);
    l.data = rs.getBytes(5);
    l.time = rs.getLong(6);
    l.version = rs.getInt(7);
    return l;
  }

  // count the #links
  @Override
  public long countLinks(String dbid, long id1, long link_type)
    throws Exception {
    while (true) {
      try {
        return countLinksImpl(dbid, id1, link_type);
      } catch (SQLException ex) {
        if (!processSQLException(ex, "countLinks")) {
          throw ex;
        }
      }
    }
  }

  private long countLinksImpl(String dbid, long id1, long link_type)
        throws Exception {
    long count = 0;
    String query = " select count from " + counttable +
                   " where id = " + id1 + " and link_type = " + link_type;

    ResultSet rs = stmt.executeQuery(query);
    boolean found = false;

    while (rs.next()) {
      // found
      if (found) {
        logger.trace("Count query 2nd row!: " + id1 + "," + link_type);
      }

      found = true;
      count = rs.getLong(1);
    }

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("Count result: " + id1 + "," + link_type +
                         " is " + found + " and " + count);
    }

    return count;
  }

  @Override
  public int bulkLoadBatchSize() {
    return bulkInsertSize;
  }

  @Override
  public void addBulkLinks(String dbid, List<Link> links, boolean noinverse)
      throws Exception {
    while (true) {
      try {
        addBulkLinksImpl(dbid, links, noinverse);
        return;
      } catch (SQLException ex) {
        if (!processSQLException(ex, "addBulkLinks")) {
          throw ex;
        }
      }
    }
  }

  private void addBulkLinksImpl(String dbid, List<Link> links, boolean noinverse)
      throws Exception {
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("addBulkLinks: " + links.size() + " links");
    }

    addLinksNoCount(dbid, links);
    conn.commit();
  }

  @Override
  public void addBulkCounts(String dbid, List<LinkCount> counts)
                                                throws Exception {
    while (true) {
      try {
        addBulkCountsImpl(dbid, counts);
        return;
      } catch (SQLException ex) {
        if (!processSQLException(ex, "addBulkCounts")) {
          throw ex;
        }
      }
    }
  }

  private void addBulkCountsImpl(String dbid, List<LinkCount> counts)
                                                throws Exception {
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("addBulkCounts: " + counts.size() + " link counts");
    }
    if (counts.size() == 0)
      return;

    String query = "UPSERT INTO " + counttable + 
        " (id, link_type, count, time, version)" +
        " VALUES(?, ?, ?, ?, ?)";
    PreparedStatement statement = conn.prepareStatement(query);

    for (LinkCount count: counts) {
      statement.setLong(1, count.id1);
      statement.setLong(2, count.link_type);
      statement.setLong(3, count.count);
      statement.setLong(4, count.time);
      statement.setLong(5, count.version);
      statement.executeUpdate();
      
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(statement.toString());
      }
    }
    conn.commit();
  }

  private void checkNodeTableConfigured() throws Exception {
    if (this.nodetable == null) {
      throw new Exception("Nodetable not specified: cannot perform node" +
          " operation");
    }
  }

  @Override
  public void resetNodeStore(String dbid, long startID) throws Exception {
    checkNodeTableConfigured();
    // FIXME : need to implement TRUNCATE TABLE.
    // Truncate table deletes all data and allows us to reset autoincrement
//    stmt.execute(String.format("TRUNCATE TABLE `%s`.`%s`;",
//                 dbid, nodetable));

    nodeIdGenerator.set(startID);
  }

  @Override
  public long addNode(String dbid, Node node) throws Exception {
    while (true) {
      try {
        return addNodeImpl(dbid, node);
      } catch (SQLException ex) {
        if (!processSQLException(ex, "addNode")) {
          throw ex;
        }
      }
    }
  }

  private long addNodeImpl(String dbid, Node node) throws Exception {
    long ids[] = bulkAddNodes(dbid, Collections.singletonList(node));
    assert(ids.length == 1);
    return ids[0];
  }

  @Override
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
    while (true) {
      try {
        return bulkAddNodesImpl(dbid, nodes);
      } catch (SQLException ex) {
        if (!processSQLException(ex, "bulkAddNodes")) {
          throw ex;
        }
      }
    }
  }

  private long[] bulkAddNodesImpl(String dbid, List<Node> nodes) throws Exception {
    checkNodeTableConfigured();

    long newIds[] = new long[nodes.size()];
    int i = 0;
    String query = "UPSERT INTO " + nodetable + 
        " (id, type, version, time, data)" +
        " VALUES(?, ?, ?, ?, ?)";
    PreparedStatement statement = conn.prepareStatement(query);

    for (Node node: nodes) {
      //long nodeID = (node.id < 0) ? nodeIdGenerator.next() : node.id;
      long nodeID = nodeIdGenerator.next();
      statement.setLong(1, nodeID);
      statement.setInt(2, node.type);
      statement.setLong(3, node.version);
      statement.setInt(4, node.time);
      statement.setBytes(5, node.data);
      statement.executeUpdate();
      newIds[i++] = nodeID;
    }
    conn.commit();
    return newIds;
  }

  @Override
  public Node getNode(String dbid, int type, long id) throws Exception {
    while (true) {
      try {
        return getNodeImpl(dbid, type, id);
      } catch (SQLException ex) {
        if (!processSQLException(ex, "getNode")) {
          throw ex;
        }
      }
    }
  }

  private Node getNodeImpl(String dbid, int type, long id) throws Exception {
    checkNodeTableConfigured();
    ResultSet rs = stmt.executeQuery(
      "SELECT id, type, version, time, data " +
      "FROM " + nodetable + " " +
      "WHERE id=" + id);
    if (rs.next()) {
      Node res = new Node(rs.getLong(1), rs.getInt(2),
           rs.getLong(3), rs.getInt(4), rs.getBytes(5));

      // Check that multiple rows weren't returned
      assert(rs.next() == false);
      rs.close();
      if (res.type != type) {
        return null;
      } else {
        return res;
      }
    }
    return null;
  }

  @Override
  public boolean updateNode(String dbid, Node node) throws Exception {
    while (true) {
      try {
        return updateNodeImpl(dbid, node);
      } catch (SQLException ex) {
        if (!processSQLException(ex, "updateNode")) {
          throw ex;
        }
      }
    }
  }

  private boolean updateNodeImpl(String dbid, Node node) throws Exception {
    checkNodeTableConfigured();
    String query = "UPSERT INTO " + nodetable +
            "(id, type, version, time, data) SELECT id, type, ?, ?, ? FROM " + nodetable +
            " WHERE id=" + node.id + " AND type=" + node.type;

    PreparedStatement statement = conn.prepareStatement(query);

    statement.setLong(1, node.version);
    statement.setInt(2, node.time);
    statement.setBytes(3, node.data);
    
    // FIXME : Is there anyway to get the real update count after commit?
    int rows = statement.executeUpdate();

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(statement.toString());
    }

    conn.commit();

    if (rows == 1) return true;
    else if (rows == 0) return false;
    else throw new Exception("Did not expect " + rows +  "affected rows: only "
        + "expected update to affect at most one row");
  }

  @Override
  public boolean deleteNode(String dbid, int type, long id) throws Exception {
    while (true) {
      try {
        return deleteNodeImpl(dbid, type, id);
      } catch (SQLException ex) {
        if (!processSQLException(ex, "deleteNode")) {
          throw ex;
        }
      }
    }
  }

  private boolean deleteNodeImpl(String dbid, int type, long id) throws Exception {
    checkNodeTableConfigured();
    stmt.executeUpdate(
        "DELETE FROM " + nodetable + " " +
        "WHERE id=" + id + " and type =" + type);
    conn.commit();
    int rows = stmt.getUpdateCount();
    if (rows == 0) {
      return false;
    } else if (rows == 1) {
      return true;
    } else {
      throw new Exception(rows + " rows modified on delete: should delete " +
                      "at most one");
    }
  }
}
