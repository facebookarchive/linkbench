package com.facebook.LinkBench;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;

public class LinkStoreMysql extends LinkStore {

  private static final boolean INTERNAL_TESTING = false;

  String assoctable;
  String counttable;
  String host;
  String user;
  String pwd;
  String port;
  int debuglevel;
  Connection conn;
  Statement stmt;

  public LinkStoreMysql() {
    super();
  }

  public LinkStoreMysql(Properties props) throws IOException {
    super();
    initialize(props, 0, 0);
  }

  public void initialize(Properties props, int currentPhase,
    int threadId) throws IOException {
    counttable = props.getProperty("counttable");
    if (counttable == null || counttable.equals("")) {
      System.err.println("Error! counttable is empty/ not found!");
      System.err.println("Please check configuration file.");
      System.exit(1);
    }

    host = props.getProperty("host");
    user = props.getProperty("user");
    pwd = props.getProperty("password");
    port = props.getProperty("port");
    if (port == null || port.equals("")) port = "3306"; //use default port
    debuglevel = Integer.parseInt(props.getProperty("debuglevel"));

    // connect
    try {
      openConnection();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }

    assoctable = props.getProperty("tablename");
  }

  // connects to test database
  private void openConnection() throws Exception {
    conn = null;
    stmt = null;
    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      conn = DriverManager.getConnection(
                          "jdbc:mysql://"+ host + ":" + port + "/" + "test" +
                          "?elideSetAutoCommits=true" +
                          "&useLocalTransactionState=true" +
                          "&allowMultiQueries=true" +
                          "&useLocalSessionState=true",
                          user, pwd);
      // System.out.println("connected");
      conn.setAutoCommit(false);
      stmt = conn.createStatement();
    }
    catch (Throwable e)
    {
      e.printStackTrace();
      return;
    }
  }

  public void clearErrors(int threadID) {
    System.err.println("Clearing region cache in threadID " + threadID);

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

    if (debuglevel > 0) {
      System.out.println("addLink " + l.id1 +
                         "." + l.id2 +
                         "." + l.link_type);
    }

    // conn.setAutoCommit(false);

    // query to insert a link;
    // if the link is already there then update its visibility
    // only update visibility; skip updating time, version, etc.
    String insert = "INSERT INTO " + dbid + "." + assoctable +
                    "(id1, id1_type, id2, id2_type, link_type, " +
                    "visibility, data, time, version) " +
                    "VALUES (" + l.id1 +
                    ", " + l.id1_type +
                    ", " + l.id2 +
                    ", " + l.id2_type +
                    ", " + l.link_type +
                    ", " + LinkStore.VISIBILITY_DEFAULT +
                    ", '" + new String(l.data) +
                    "', " + l.time +
                    ", "  + l.version +
                    ") ON DUPLICATE KEY UPDATE " +
                    "visibility = " + LinkStore.VISIBILITY_DEFAULT;

    if (debuglevel > 1) {
      System.out.println(insert);
    }

    int nrows = stmt.executeUpdate(insert);

    if (debuglevel > 1) {
      System.out.println("nrows = " + nrows);
    }

    // based on nrows, determine whether the previous query was an insert
    // or update
    boolean update_count = false, update_data = false;

    switch (nrows) {
      case 1:
        // a new row was inserted --> need to update counttable
        update_count = true;
        break;

      case 2:
        // nothing changed (a row is found but its visibility was
        // already VISIBILITY_DEFAULT)
        // --> need to update other data
        update_data = true;
        break;

      case 3:
        // a visibility was changed from VISIBILITY_HIDDEN to DEFAULT
        // --> need to update both counttable and other data
        update_count = true;
        update_data = true;
        break;

      default:
        System.err.print("SQL Error");
        throw new Exception("Value of affected-rows number is not valid" +
                            nrows);
    }

    if (update_count) {
      // query to update counttable
      // if (id, id_type) is not there yet, add a new record with count = 1
      String updatecount = "INSERT INTO " + dbid + "." + counttable +
                      "(id, id_type, link_type, count, time, version) " +
                      "VALUES (" + l.id1 +
                      ", " + l.id1_type +
                      ", " + l.link_type +
                      ", 1" +
                      ", " + l.time +
                      ", " + l.version + ") " +
                      "ON DUPLICATE KEY UPDATE count = count + 1;";

      if (debuglevel > 1) {
        System.out.println(updatecount);
      }

      stmt.executeUpdate(updatecount);
    }

    if (update_data) {
      // query to update link data (the first query only updates visibility)
      String updatedata = "UPDATE " + dbid + "." + assoctable +
                  " SET id1_type = " + l.id1_type +
                  ", id2_type = " + l.id2_type +
                  ", visibility = " + LinkStore.VISIBILITY_DEFAULT +
                  ", data = '" + new String(l.data) +
                  "', time = " + l.time +
                  ", version = " + l.version +
                  " WHERE id1 = " + l.id1 +
                  " AND id2 = " + l.id2 +
                  " AND link_type = " + l.link_type + ";";

      if (debuglevel > 1) {
        System.out.println(updatedata);
      }

      stmt.executeUpdate(updatedata);
    }

    if (INTERNAL_TESTING) {
      testCount(stmt, dbid, assoctable, counttable, l.id1, l.link_type);
    }

    conn.commit();
    // conn.setAutoCommit(true);
  }


  public void deleteLink(String dbid, long id1, long link_type, long id2,
                         boolean noinverse, boolean expunge)
    throws Exception {

    if (debuglevel > 0) {
      System.out.println("deleteLink " + id1 +
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
                    " FROM " + dbid + "." + assoctable +
                    " WHERE id1 = " + id1 +
                    " AND id2 = " + id2 +
                    " AND link_type = " + link_type + ";";

    if (debuglevel > 1) {
      System.out.println(select);
    }

    ResultSet result = stmt.executeQuery(select);

    int visibility = -1;
    while (result.next()) {
      visibility = result.getInt("visibility");
    }

    if (debuglevel > 1) {
      System.out.println("visibility = " + visibility);
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
        delete = "UPDATE " + dbid + "." + assoctable +
                 " SET visibility = " + VISIBILITY_HIDDEN +
                 " WHERE id1 = " + id1 +
                 " AND id2 = " + id2 +
                 " AND link_type = " + link_type + ";";
      } else {
        delete = "DELETE FROM " + dbid + "." + assoctable +
                 " WHERE id1 = " + id1 +
                 " AND id2 = " + id2 +
                 " AND link_type = " + link_type + ";";
      }

      if (debuglevel > 1) {
        System.out.println(delete);
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

      if (debuglevel > 1) {
        System.out.println(update);
      }

      stmt.executeUpdate(update);
    }
    else {
      throw new Exception("Value of visibility is not valid: " +
                          visibility);
    }

    if (INTERNAL_TESTING) {
      testCount(stmt, dbid, assoctable, counttable, id1, link_type);
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
    String query = " select id1, id2, link_type, visibility, data, time, " +
                   " version from " + dbid + "." + assoctable +
                   " where id1 = " + id1 + " and id2 = " + id2 +
                   " and link_type = " + link_type + "; commit;";


    if (debuglevel > 1) {
      System.out.println("Query is " + query);
    }

    ResultSet rs = stmt.executeQuery(query);
    boolean found = false;

    while (rs.next()) {
      // found
      found = true;
    }

    if (debuglevel > 1) {
      System.out.println("Lookup result: " + id1 + "," + link_type + "," + id2 +
                         " is " + found);
    }

    Link l = new Link();
    l.id1 = id1;
    l.id2 = id2;
    l.link_type = link_type;

    return l;
  }


  // lookup using just id1, type
  public Link[] getLinkList(String dbid, long id1, long link_type)
    throws Exception {
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, 10000);
  }

  public Link[] getLinkList(String dbid, long id1, long link_type,
                            long minTimestamp, long maxTimestamp,
                            int offset, int limit)
    throws Exception {
    String query = " select id1, id2, link_type, visibility, data, time, " +
                   " version from " + dbid + "." + assoctable +
                   " where id1 = " + id1 + " and link_type = " + link_type +
                   " and time >= " + minTimestamp +
                   " and time <= " + maxTimestamp +
                   " and visibility = " + LinkStore.VISIBILITY_DEFAULT +
                   " order by time desc " +
                   " limit " + offset + "," + limit + "; commit;";

    if (debuglevel > 1) {
      System.out.println("Query is " + query);
    }

    ResultSet rs = stmt.executeQuery(query);
    boolean found = false;

    int count = 0;
    while (rs.next()) {
      count++;
    }

    if (debuglevel > 1) {
      System.out.println("Range lookup result: " + id1 + "," + link_type +
                         " is " + count);
    }

    // TODO Populate individual links
    return (count > 0 ? new Link[count] : null);
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
        System.out.println("Count query 2nd row!: " + id1 + "," + link_type);
      }

      found = true;
      count = rs.getLong(1);
    }

    if (debuglevel > 1) {
      System.out.println("Count result: " + id1 + "," + link_type +
                         " is " + found + " and " + count);
    }

    return count;
  }


}
