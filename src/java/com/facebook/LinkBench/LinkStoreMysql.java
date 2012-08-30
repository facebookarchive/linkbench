package com.facebook.LinkBench;

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
import java.util.List;
import java.util.Properties;

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
    counttable = props.getProperty(Config.COUNT_TABLE);
    if (counttable == null || counttable.equals("")) {
      String msg = "Error! counttable is empty/ not found!"
          + "Please check configuration file.";
      logger.error(msg);
      throw new RuntimeException(msg);
    }
    
    nodetable = props.getProperty(Config.NODE_TABLE);
    if (nodetable.equals("")) {
      // For now, don't assume that nodetable is provided
      String msg = "Error! nodetable is empty!"
          + "Please check configuration file.";
      logger.error(msg);
      throw new RuntimeException(msg);
    }
    
    host = props.getProperty(CONFIG_HOST);
    user = props.getProperty(CONFIG_USER);
    pwd = props.getProperty(CONFIG_PASSWORD);
    port = props.getProperty(CONFIG_PORT);
    if (port == null || port.equals("")) port = "3306"; //use default port
    debuglevel = ConfigUtil.getDebugLevel(props);
    phase = currentPhase;

    if (props.containsKey(CONFIG_BULK_INSERT_BATCH)) {
      bulkInsertSize = Integer.parseInt(
                        props.getProperty(CONFIG_BULK_INSERT_BATCH));
    }
    if (props.containsKey(CONFIG_DISABLE_BINLOG_LOAD)) {
      disableBinLogForLoad = Boolean.parseBoolean(
                        props.getProperty(CONFIG_DISABLE_BINLOG_LOAD));
    }
    
    // connect
    try {
      openConnection();
    } catch (SQLException e) {
      logger.error("error connecting to database:"
                  + e.getMessage());
      System.exit(1);
    } catch (Exception e) {
      logger.error(e);
      System.exit(1);
    }

    linktable = props.getProperty(Config.LINK_TABLE);
  }

  // connects to test database
  private void openConnection() throws Exception {
    conn = null;
    stmt = null;
    
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    conn = DriverManager.getConnection(
                        "jdbc:mysql://"+ host + ":" + port + "/" + "test" +
                        "?elideSetAutoCommits=true" +
                        "&useLocalTransactionState=true" +
                        "&allowMultiQueries=true" +
                        "&useLocalSessionState=true",
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
    logger.info("Clearing region cache in threadID " + threadID);

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

  // get count for testing purpose
  private void testCount(Statement stmt, String dbid,
                         String assoctable, String counttable,
                         long id, long link_type)
    throws Exception {

    String select1 = "SELECT COUNT(id2)" +
                     " FROM " + dbid + "." + assoctable +
                     " WHERE id1 = " + id +
                     " AND link_type = " + link_type +
                     " AND visibility = " + LinkStore.VISIBILITY_DEFAULT;

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


  public void addLink(String dbid, Link l, boolean noinverse)
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
    boolean update_data = false;
    int update_count = 0;

    switch (nrows) {
      case 1:
        // a new row was inserted --> need to update counttable
        if (l.visibility == VISIBILITY_DEFAULT) {
          update_count = 1;
        }
        break;

      case 2:
        // nothing changed (a row is found but its visibility was
        // already VISIBILITY_DEFAULT)
        // --> need to update other data
        update_data = true;
        break;

      case 3:
        // a visibility was changed from VISIBILITY_HIDDEN to DEFAULT 
        // or vice-versa
        // --> need to update both counttable and other data
        if (l.visibility == VISIBILITY_DEFAULT) {
          update_count = 1;
        } else {
          update_count = -1;
        }
        update_data = true;
        break;

      default:
        String msg = "Value of affected-rows number is not valid" + nrows;
        logger.error("SQL Error: " + msg);
        throw new Exception(msg);
    }

    if (update_count != 0) {
      int base_count = update_count < 0 ? 0 : 1;
      // query to update counttable
      // if (id, id_type) is not there yet, add a new record with count = 1
      String updatecount = "INSERT INTO " + dbid + "." + counttable +
                      "(id, id_type, link_type, count, time, version) " +
                      "VALUES (" + l.id1 +
                      ", " + l.id1_type +
                      ", " + l.link_type +
                      ", " + base_count +
                      ", " + l.time +
                      ", " + l.version + ") " +
                      "ON DUPLICATE KEY UPDATE count = count + " +
                      update_count + ";";

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(updatecount);
      }

      stmt.executeUpdate(updatecount);
    }

    if (update_data) {
      // query to update link data (the first query only updates visibility)
      // TODO: why is this necessary?
      String updatedata = "UPDATE " + dbid + "." + linktable +
                  " SET id1_type = " + l.id1_type +
                  ", id2_type = " + l.id2_type +
                  ", visibility = " + LinkStore.VISIBILITY_DEFAULT +
                  ", data = " +  stringLiteral(l.data)+
                  ", time = " + l.time +
                  ", version = " + l.version +
                  " WHERE id1 = " + l.id1 +
                  " AND id2 = " + l.id2 +
                  " AND link_type = " + l.link_type + ";";

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace(updatedata);
      }

      stmt.executeUpdate(updatedata);
    }

    if (INTERNAL_TESTING) {
      testCount(stmt, dbid, linktable, counttable, l.id1, l.link_type);
    }

    conn.commit();
    // conn.setAutoCommit(true);
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
                    "(id1, id1_type, id2, id2_type, link_type, " +
                    "visibility, data, time, version) VALUES ");
    boolean first = true;
    for (Link l : links) {
      if (first) {
        first = false;
      } else {
        sb.append(',');
      }
      sb.append("(" + l.id1 + 
          ", " + l.id1_type + 
          ", " + l.id2 + 
          ", " + l.id2_type + 
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

  public void deleteLink(String dbid, long id1, long link_type, long id2,
                         boolean noinverse, boolean expunge)
    throws Exception {

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
    while (result.next()) {
      visibility = result.getInt("visibility");
    }

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(String.format("(%d, %d, %d) visibility = %d",
                id1, link_type, id2, visibility));
    }

    if (visibility == -1) {
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
                      " (id, id_type, link_type, count, time, version) " +
                      "VALUES (" + id1 +
                      ", " + LinkStore.ID1_TYPE +
                      ", " + link_type +
                      ", 0" +
                      ", " + currentTime +
                      ", " + 0 + ") " +
                      "ON DUPLICATE KEY UPDATE" +
                      " id_type = " + LinkStore.ID1_TYPE +
                      ", count = IF (count = 0, 0, count - 1)" +
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
  }


  public void updateLink(String dbid, Link l, boolean noinverse)
    throws Exception {

    addLink(dbid, l, noinverse);
  }


  // lookup using id1, type, id2
  public Link getLink(String dbid, long id1, long link_type, long id2)
    throws Exception {
    Link res[] = multigetLinks(dbid, id1, link_type, new long[] {id2});
    if (res == null) return null;
    assert(res.length <= 1);
    return res.length == 0 ? null : res[0];
  }


  @Override
  public Link[] multigetLinks(String dbid, long id1, long link_type, 
                              long[] id2s) throws Exception {
    StringBuilder querySB = new StringBuilder();
    querySB.append(" select id1, id2, link_type, id1_type, id2_type," +
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
  public Link[] getLinkList(String dbid, long id1, long link_type)
    throws Exception {
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
  }

  public Link[] getLinkList(String dbid, long id1, long link_type,
                            long minTimestamp, long maxTimestamp,
                            int offset, int limit)
    throws Exception {
    String query = " select id1, id2, link_type, id1_type, id2_type," +
    		           " visibility, data, time," +
                   " version from " + dbid + "." + linktable +
                   " where id1 = " + id1 + " and link_type = " + link_type +
                   " and time >= " + minTimestamp +
                   " and time <= " + maxTimestamp +
                   " and visibility = " + LinkStore.VISIBILITY_DEFAULT +
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
    l.id1_type = rs.getInt(4);
    l.id2_type = rs.getInt(5);
    l.visibility = rs.getByte(6);
    l.data = rs.getBytes(7);
    l.time = rs.getLong(8);
    l.version = rs.getInt(9);
    return l;
  }

  // count the #links
  public long countLinks(String dbid, long id1, long link_type)
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
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("addBulkLinks: " + links.size() + " links");
    }
    
    addLinksNoCount(dbid, links);
    conn.commit();
  }

  @Override
  public void addBulkCounts(String dbid, List<LinkCount> counts)
                                                throws Exception {
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("addBulkCounts: " + counts.size() + " link counts");
    }
    if (counts.size() == 0)
      return;
    
    StringBuilder sqlSB = new StringBuilder(); 
    sqlSB.append("INSERT INTO " + dbid + "." + counttable +
        "(id, id_type, link_type, count, time, version) " +
        "VALUES ");
    boolean first = true;
    for (LinkCount count: counts) {
      if (first) {
        first = false;
      } else {
        sqlSB.append(",");
      }
      sqlSB.append("(" + count.id1 +
        ", " + count.id1_type +
        ", " + count.link_type +
        ", " + count.count +
        ", " + count.time +
        ", " + count.version + ")");
    }
    sqlSB.append("ON DUPLICATE KEY UPDATE"
        + " count = VALUES(count), time = VALUES(time),"
        + " version = VALUES(version)");
        
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
    long ids[] = bulkAddNodes(dbid, Collections.singletonList(node));
    assert(ids.length == 1);
    return ids[0];
  }

  @Override
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
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
