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
package com.facebook.LinkBench.store.mysql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import com.facebook.LinkBench.*;
import com.facebook.LinkBench.store.GraphStore;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class LinkStoreMysql extends GraphStore {

  /* MySql database server configuration keys */
  public static final String CONFIG_HOST = "host";
  public static final String CONFIG_PORT = "port";
  public static final String CONFIG_USER = "user";
  public static final String CONFIG_PASSWORD = "password";
  public static final String CONFIG_BULK_INSERT_BATCH = "mysql_bulk_insert_batch";
  public static final String CONFIG_DISABLE_BINLOG_LOAD = "mysql_disable_binlog_load";

  public static final int DEFAULT_BULKINSERT_SIZE = 1024;

  private static final boolean INTERNAL_TESTING = false;

  String linktable;
  String counttable;
  String nodetable;

  String host;
  String user;
  String pwd;
  String port;
  String defaultDB;

  Level debuglevel;
  Connection conn;
  Statement stmt;

  private Phase phase;

  int bulkInsertSize = DEFAULT_BULKINSERT_SIZE;
  // Optional optimization: disable binary logging
  boolean disableBinLogForLoad = false;

  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

  public LinkStoreMysql() {
    super();
  }

  public LinkStoreMysql(Properties props) throws IOException, Exception {
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
      // For now, don't assume that nodetable is provided
      String msg = "Error! " + Config.NODE_TABLE + " is empty!"
          + "Please check configuration file.";
      logger.error(msg);
      throw new RuntimeException(msg);
    }

    host = ConfigUtil.getPropertyRequired(props, CONFIG_HOST);
    user = ConfigUtil.getPropertyRequired(props, CONFIG_USER);
    pwd = ConfigUtil.getPropertyRequired(props, CONFIG_PASSWORD);
    port = props.getProperty(CONFIG_PORT);
    defaultDB = ConfigUtil.getPropertyRequired(props, Config.DBID);

    if (port == null || port.equals("")) port = "3306"; //use default port
    debuglevel = ConfigUtil.getDebugLevel(props);
    phase = currentPhase;

    if (props.containsKey(CONFIG_BULK_INSERT_BATCH)) {
      bulkInsertSize = ConfigUtil.getInt(props, CONFIG_BULK_INSERT_BATCH);
    }
    if (props.containsKey(CONFIG_DISABLE_BINLOG_LOAD)) {
      disableBinLogForLoad = ConfigUtil.getBool(props,
                                      CONFIG_DISABLE_BINLOG_LOAD);
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

    String jdbcUrl = "jdbc:mysql://"+ host + ":" + port + "/";
    if (defaultDB != null) {
      jdbcUrl += defaultDB;
    }

    Class.forName("com.mysql.jdbc.Driver").newInstance();
    conn = DriverManager.getConnection(
                        jdbcUrl +
                        "?elideSetAutoCommits=true" +
                        "&useLocalTransactionState=true" +
                        "&allowMultiQueries=true" +
                        "&useLocalSessionState=true" +
   /* Need affected row count from queries to distinguish updates/inserts
    * consistently across different MySql versions (see MySql bug 46675) */
                        "&useAffectedRows=true",
                        user, pwd);
    //System.err.println("connected");
    conn.setAutoCommit(false);
    stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_READ_ONLY);

    if (phase == Phase.LOAD && disableBinLogForLoad) {
      // Turn binary logging off for duration of connection
      stmt.executeUpdate("SET SESSION sql_log_bin=0");
    }
  }

  @Override
  public void close() {
    try {
      if (stmt != null) stmt.close();
      if (conn != null) conn.close();
    } catch (SQLException e) {
      logger.error("Error while closing MySQL connection: ", e);
    }
  }

  public void clearErrors(int threadID) {
    logger.info("Reopening MySQL connection in threadID " + threadID);

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
   * Set of all JDBC SQLState strings that indicate a transient MySQL error
   * that should be handled by retrying
   */
  private static final HashSet<String> retrySQLStates = populateRetrySQLStates();

  /**
   *  Populate retrySQLStates
   *  SQLState codes are defined in MySQL Connector/J documentation:
   *  http://dev.mysql.com/doc/refman/5.6/en/connector-j-reference-error-sqlstates.html
   */
  private static HashSet<String> populateRetrySQLStates() {
    HashSet<String> states = new HashSet<String>();
    states.add("41000"); // ER_LOCK_WAIT_TIMEOUT
    states.add("40001"); // ER_LOCK_DEADLOCK
    return states;
  }

  /**
   * Handle SQL exception by logging error and selecting how to respond
   * @param ex SQLException thrown by MySQL JDBC driver
   * @return true if transaction should be retried
   */
  private boolean processSQLException(SQLException ex, String op) {
    boolean retry = retrySQLStates.contains(ex.getSQLState());
    String msg = "SQLException thrown by MySQL driver during execution of " +
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

  // get count for testing purpose
  private void testCount(Statement stmt, String dbid,
                         String assoctable, String counttable,
                         long id, long link_type)
    throws Exception {

    String select1 = "SELECT COUNT(id2)" +
                     " FROM " + dbid + "." + assoctable +
                     " WHERE id1 = " + id +
                     " AND link_type = " + link_type +
                     " AND visibility = " + VISIBILITY_DEFAULT;

    String select2 = "SELECT COALESCE (SUM(count), 0)" +
                     " FROM " + dbid + "." + counttable +
                     " WHERE id = " + id +
                     " AND link_type = " + link_type;

    String verify = "SELECT IF ((" + select1 +
                    ") = (" + select2 + "), 1, 0) as result";

    ResultSet result = stmt.executeQuery(verify);

    int ret = -1;
    while (result.next()) {
      ret = result.getInt("result");
    }

    if (ret != 1) {
      throw new Exception("Data inconsistency between " + assoctable +
                          " and " + counttable);
    }
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

    // conn.setAutoCommit(false);


    // if the link is already there then update its visibility
    // only update visibility; skip updating time, version, etc.

    int nrows = addLinksNoCount(dbid, Collections.singletonList(l));

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("nrows = " + nrows);
    }

    // based on nrows, determine whether the previous query was an insert
    // or update
    boolean row_found;
    boolean update_data = false;
    int update_count = 0;

    switch (nrows) {
      case 1:
        // a new row was inserted --> need to update counttable
        if (l.visibility == VISIBILITY_DEFAULT) {
          update_count = 1;
        }
        row_found = false;
        break;

      case 0:
        // A row is found but its visibility was unchanged
        // --> need to update other data
        update_data = true;
        row_found = true;
        break;

      case 2:
        // a visibility was changed from VISIBILITY_HIDDEN to DEFAULT
        // or vice-versa
        // --> need to update both counttable and other data
        if (l.visibility == VISIBILITY_DEFAULT) {
          update_count = 1;
        } else {
          update_count = -1;
        }
        update_data = true;
        row_found = true;
        break;

      default:
        String msg = "Value of affected-rows number is not valid" + nrows;
        logger.error("SQL Error: " + msg);
        throw new Exception(msg);
    }

    if (update_count != 0) {
      int base_count = update_count < 0 ? 0 : 1;
      // query to update counttable
      // if (id, link_type) is not there yet, add a new record with count = 1
      String updatecount = "INSERT INTO " + dbid + "." + counttable +
                      "(id, link_type, count, time, version) " +
                      "VALUES (" + l.id1 +
                      ", " + l.link_type +
                      ", " + base_count +
                      ", " + l.time +
                      ", " + l.version + ") " +
                      "ON DUPLICATE KEY UPDATE count = count + " +
                      update_count + ";";

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(updatecount);
      }

      // This is the last statement of transaction - append commit to avoid
      // extra round trip
      if (!update_data) {
        updatecount += " commit;";
      }
      stmt.executeUpdate(updatecount);
    }

    if (update_data) {
      // query to update link data (the first query only updates visibility)
      String updatedata = "UPDATE " + dbid + "." + linktable + " SET" +
                  " visibility = " + l.visibility +
                  ", data = " +  stringLiteral(l.data)+
                  ", time = " + l.time +
                  ", version = " + l.version +
                  " WHERE id1 = " + l.id1 +
                  " AND id2 = " + l.id2 +
                  " AND link_type = " + l.link_type + "; commit;";
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(updatedata);
      }

      stmt.executeUpdate(updatedata);
    }

    if (INTERNAL_TESTING) {
      testCount(stmt, dbid, linktable, counttable, l.id1, l.link_type);
    }
    return row_found;
  }

  /**
   * Internal method: add links without updating the count
   * @param dbid
   * @param links
   * @return
   * @throws SQLException
   */
  private int addLinksNoCount(String dbid, List<Link> links)
      throws SQLException {
    if (links.size() == 0)
      return 0;

    // query to insert a link;
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO " + dbid + "." + linktable +
                    "(id1, id2, link_type, " +
                    "visibility, data, time, version) VALUES ");
    boolean first = true;
    for (Link l : links) {
      if (first) {
        first = false;
      } else {
        sb.append(',');
      }
      sb.append("(" + l.id1 +
          ", " + l.id2 +
          ", " + l.link_type +
          ", " + l.visibility +
          ", " + stringLiteral(l.data) +
          ", " + l.time + ", " +
          l.version + ")");
    }
    sb.append(" ON DUPLICATE KEY UPDATE visibility = VALUES(visibility)");
    String insert = sb.toString();
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(insert);
    }

    int nrows = stmt.executeUpdate(insert);
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

    // First do a select to check if the link is not there, is there and
    // hidden, or is there and visible;
    // Result could be either NULL, VISIBILITY_HIDDEN or VISIBILITY_DEFAULT.
    // In case of VISIBILITY_DEFAULT, later we need to mark the link as
    // hidden, and update counttable.
    String select = "SELECT visibility" +
                    " FROM " + dbid + "." + linktable +
                    " WHERE id1 = " + id1 +
                    " AND id2 = " + id2 +
                    " AND link_type = " + link_type + ";";

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(select);
    }

    ResultSet result = stmt.executeQuery(select);

    int visibility = -1;
    boolean found = false;
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
    }
    else if (visibility == VISIBILITY_HIDDEN) {
      // do nothing
    }
    else if (visibility == VISIBILITY_DEFAULT) {
      // either delete or mark the link as hidden
      String delete;

      if (!expunge) {
        delete = "UPDATE " + dbid + "." + linktable +
                 " SET visibility = " + VISIBILITY_HIDDEN +
                 " WHERE id1 = " + id1 +
                 " AND id2 = " + id2 +
                 " AND link_type = " + link_type + ";";
      } else {
        delete = "DELETE FROM " + dbid + "." + linktable +
                 " WHERE id1 = " + id1 +
                 " AND id2 = " + id2 +
                 " AND link_type = " + link_type + ";";
      }

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(delete);
      }

      stmt.executeUpdate(delete);

      // update count table
      // * if found (id1, link_type) in count table, set
      //   count = (count == 1) ? 0) we decrease the value of count
      //   column by 1;
      // * otherwise, insert new link with count column = 0
      long currentTime = (new Date()).getTime();
      String update = "INSERT INTO " + dbid + "." + counttable +
                      " (id, link_type, count, time, version) " +
                      "VALUES (" + id1 +
                      ", " + link_type +
                      ", 0" +
                      ", " + currentTime +
                      ", " + 0 + ") " +
                      "ON DUPLICATE KEY UPDATE" +
                      " count = IF (count = 0, 0, count - 1)" +
                      ", time = " + currentTime +
                      ", version = version + 1;";

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(update);
      }

      stmt.executeUpdate(update);
    }
    else {
      throw new Exception("Value of visibility is not valid: " +
                          visibility);
    }

    if (INTERNAL_TESTING) {
      testCount(stmt, dbid, linktable, counttable, id1, link_type);
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
        " version from " + dbid + "." + linktable +
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

    querySB.append("); commit;");
    String query = querySB.toString();

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("Query is " + query);
    }

    ResultSet rs = stmt.executeQuery(query);

    // Get the row count to allocate result array
    assert(rs.getType() != ResultSet.TYPE_FORWARD_ONLY);
    rs.last();
    int count = rs.getRow();
    rs.beforeFirst();

    Link results[] = new Link[count];
    int i = 0;
    while (rs.next()) {
      Link l = createLinkFromRow(rs);
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("Lookup result: " + id1 + "," + link_type + "," +
                  l.id2 + " found");
      }
      results[i++] = l;
    }
    return results;
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
    String query = " select id1, id2, link_type," +
                   " visibility, data, time," +
                   " version from " + dbid + "." + linktable +
                   " where id1 = " + id1 + " and link_type = " + link_type +
                   " and time >= " + minTimestamp +
                   " and time <= " + maxTimestamp +
                   " and visibility = " + VISIBILITY_DEFAULT +
                   " order by time desc " +
                   " limit " + offset + "," + limit + "; commit;";

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("Query is " + query);
    }

    ResultSet rs = stmt.executeQuery(query);

    // Find result set size
    // be sure we fast forward to find result set size
    assert(rs.getType() != ResultSet.TYPE_FORWARD_ONLY);
    rs.last();
    int count = rs.getRow();
    rs.beforeFirst();

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("Range lookup result: " + id1 + "," + link_type +
                         " is " + count);
    }
    if (count == 0) {
      return null;
    }

    // Fetch the link data
    Link links[] = new Link[count];
    int i = 0;
    while (rs.next()) {
      Link l = createLinkFromRow(rs);
      links[i] = l;
      i++;
    }
    assert(i == count);
    return links;
  }

  private Link createLinkFromRow(ResultSet rs) throws SQLException {
    Link l = new Link();
    l.id1 = rs.getLong(1);
    l.id2 = rs.getLong(2);
    l.link_type = rs.getLong(3);
    l.visibility = rs.getByte(4);
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
    String query = " select count from " + dbid + "." + counttable +
                   " where id = " + id1 + " and link_type = " + link_type + "; commit;";

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

    StringBuilder sqlSB = new StringBuilder();
    sqlSB.append("REPLACE INTO " + dbid + "." + counttable +
        "(id, link_type, count, time, version) " +
        "VALUES ");
    boolean first = true;
    for (LinkCount count: counts) {
      if (first) {
        first = false;
      } else {
        sqlSB.append(",");
      }
      sqlSB.append("(" + count.id1 +
        ", " + count.link_type +
        ", " + count.count +
        ", " + count.time +
        ", " + count.version + ")");
    }

    String sql = sqlSB.toString();
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(sql);
    }
    stmt.executeUpdate(sql);
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
    // Truncate table deletes all data and allows us to reset autoincrement
    stmt.execute(String.format("TRUNCATE TABLE `%s`.`%s`;",
                 dbid, nodetable));
    stmt.execute(String.format("ALTER TABLE `%s`.`%s` " +
        "AUTO_INCREMENT = %d;", dbid, nodetable, startID));
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
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO `" + dbid + "`.`" + nodetable + "` " +
        "(type, version, time, data) " +
        "VALUES ");
    boolean first = true;
    for (Node node: nodes) {
      if (first) {
        first = false;
      } else {
        sql.append(",");
      }
      sql.append("(" + node.type + "," + node.version +
          "," + node.time + "," + stringLiteral(node.data) + ")");
    }
    sql.append("; commit;");
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(sql);
    }
    stmt.executeUpdate(sql.toString(), Statement.RETURN_GENERATED_KEYS);
    ResultSet rs = stmt.getGeneratedKeys();

    long newIds[] = new long[nodes.size()];
    // Find the generated id
    int i = 0;
    while (rs.next() && i < nodes.size()) {
      newIds[i++] = rs.getLong(1);
    }

    if (i != nodes.size()) {
      throw new Exception("Wrong number of generated keys on insert: "
          + " expected " + nodes.size() + " actual " + i);
    }

    assert(!rs.next()); // check done
    rs.close();

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
      "FROM `" + dbid + "`.`" + nodetable + "` " +
      "WHERE id=" + id + "; commit;");
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
    String sql = "UPDATE `" + dbid + "`.`" + nodetable + "`" +
            " SET " + "version=" + node.version + ", time=" + node.time
                   + ", data=" + stringLiteral(node.data) +
            " WHERE id=" + node.id + " AND type=" + node.type + "; commit;";

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(sql);
    }

    int rows = stmt.executeUpdate(sql);

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
    int rows = stmt.executeUpdate(
        "DELETE FROM `" + dbid + "`.`" + nodetable + "` " +
        "WHERE id=" + id + " and type =" + type + "; commit;");

    if (rows == 0) {
      return false;
    } else if (rows == 1) {
      return true;
    } else {
      throw new Exception(rows + " rows modified on delete: should delete " +
                      "at most one");
    }
  }

  /**
   * Convert a byte array into a valid mysql string literal, assuming that
   * it will be inserted into a column with latin-1 encoding.
   * Based on information at
   * http://dev.mysql.com/doc/refman/5.1/en/string-literals.html
   * @param arr
   * @return
   */
  private static String stringLiteral(byte arr[]) {
    CharBuffer cb = Charset.forName("ISO-8859-1").decode(ByteBuffer.wrap(arr));
    StringBuilder sb = new StringBuilder();
    sb.append('\'');
    for (int i = 0; i < cb.length(); i++) {
      char c = cb.get(i);
      switch (c) {
        case '\'':
          sb.append("\\'");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\0':
          sb.append("\\0");
          break;
        case '\b':
          sb.append("\\b");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (Character.getNumericValue(c) < 0) {
            // Fall back on hex string for values not defined in latin-1
            return hexStringLiteral(arr);
          } else {
            sb.append(c);
          }
      }
    }
    sb.append('\'');
    return sb.toString();
  }

  /**
   * Create a mysql hex string literal from array:
   * E.g. [0xf, bc, 4c, 4] converts to x'0fbc4c03'
   * @param arr
   * @return the mysql hex literal including quotes
   */
  private static String hexStringLiteral(byte[] arr) {
    StringBuilder sb = new StringBuilder();
    sb.append("x'");
    for (int i = 0; i < arr.length; i++) {
      byte b = arr[i];
      int lo = b & 0xf;
      int hi = (b >> 4) & 0xf;
      sb.append(Character.forDigit(hi, 16));
      sb.append(Character.forDigit(lo, 16));
    }
    sb.append("'");
    return sb.toString();
  }
}
