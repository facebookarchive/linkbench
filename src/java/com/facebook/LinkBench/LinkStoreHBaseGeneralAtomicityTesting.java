
package com.facebook.LinkBench;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;

/*
  This class is a stress test for verifying HBase API get/put operations. It
  basically checks, at a reasonable frequency, whether these opertations
  behave as expected.

  At first, run CreateTaoTable in hbase/shell to create the required table,
  then run LinkBenchDriver with config option:
    store="HBaseGeneralAtomicityTesting" (in file LinkConfig.peroperties)

  Other configs (in file LinkConfig.properties) that need to be set:
  + table_name: should be the same as the name used with CreateTaoTable
  in the previous step.
  + id2gen_config: this specifies genereting disjoint id2 for threads. Must
  be set to 1.
  + countlink, getlink, getlinklist: must be set to 0.
  + sleeprate
  + sleeptime
 */


public class LinkStoreHBaseGeneralAtomicityTesting extends LinkStore {

  private final boolean DEBUG = false;

  HTable table;
  Level debuglevel;
  ArrayList<String> columnfamilies;

  Phase currentphase;
  int threadid;
  String threadname;

  double sleeprate;
  int sleeptime;
  int maxsleepingthreads;

  static int counter = 0;
  static Object lock = new Object();

  // Caution: Because character dot (.) is used as a separation indicator,
  //  there must be no (.) in the link data.
  // If this property is violated,
  //  an exception will be thrown in method bytesToLink(...)
  private byte[] linkToBytes(Link a) {
    String temp = Long.toString(a.id1) + "." +
                  Long.toString(a.link_type) + "." +
                  Long.toString(a.id2) + "." +
                  Byte.toString(a.visibility) + "." +
                  Bytes.toString(a.data) + "." +
                  // there must be no (.) in a.data
                  Integer.toString(a.version) + "." +
                  Long.toString(a.time) + ".";
    return Bytes.toBytes(temp);
  }

  // concatanate id1, link_typ1, id2 separated by "."
  private String combine(long id1, long link_type, long id2) {
    String temp = Long.toString(id1) + "." +
                  Long.toString(link_type) + "." +
                  Long.toString(id2);
    return temp;
  }

  private Link bytesToLink(byte[] blink) throws Exception {
    String slink = new String(blink);
    String[] tokens = slink.split(".");
    assertTrue(tokens.length == 9, "wrong link format");
    // number of identities in a link must be 9

    Link a = new Link();
    a.id1 = Long.parseLong(tokens[0]);
    a.link_type = Long.parseLong(tokens[1]);
    a.id2 = Long.parseLong(tokens[2]);
    a.visibility = Byte.parseByte(tokens[3]);
    a.data = tokens[4].getBytes();
    a.version = Integer.parseInt(tokens[5]);
    a.time = Long.parseLong(tokens[6]);
    return a;
  }

  private String bytesToString(byte[] value) {
    String st = new String(value);
    return st;
  }

  private void assertTrue(boolean expression, String message)
    throws Exception {

      if (!expression) {
        System.err.println("-------------------------------------------");
        System.err.println("Test failure: " + message);
        (new Exception()).printStackTrace();
        System.exit(1);
      }
    }

  /*
   * Constructor
   */
  public LinkStoreHBaseGeneralAtomicityTesting(
      Phase input_currentphase, int input_threadid,
      Properties props) throws IOException {
      initialize(props, input_currentphase, input_threadid);
  }

  public LinkStoreHBaseGeneralAtomicityTesting() {
    super();
  }

  @Override
  public void initialize(Properties props, Phase currentphase,
    int threadid) throws IOException {
      this.currentphase = currentphase;
      this.threadid = threadid;

      if (currentphase == Phase.LOAD) {
        threadname = "Loader " + threadid;
      }
      else if (currentphase == Phase.REQUEST) {
        threadname = "Requester " + threadid;
      }
      else {
        System.err.println("Fatal error: Phase " + currentphase +
            "does not exists.");
        System.exit(1);
      }


      Configuration conf = HBaseConfiguration.create();
      String tablename = ConfigUtil.getPropertyRequired(props, Config.LINK_TABLE);
      table = new HTable(conf, tablename);

      debuglevel = ConfigUtil.getDebugLevel(props);
      sleeprate = ConfigUtil.getDouble(props, "sleeprate");
      sleeptime = ConfigUtil.getInt(props, "sleeptime");
      maxsleepingthreads = ConfigUtil.getInt(props, "maxsleepingthreads");

      if (ConfigUtil.getInt(props, "id2gen_config") != 1) {
        System.err.println("Fatal error: id2gen_config must be 1.");
        System.err.println("Please check config file.");
        System.exit(1);
      }

      // create a list that stores column family names of assoc_tien
      columnfamilies = new ArrayList<String>();
      columnfamilies.add("cf1");
      columnfamilies.add("cf2");
      columnfamilies.add("cf3");
    }

  @Override
  public void close() {
    //TODO
  }

  /*
   * Interface implementation
   */
  @Override
  public void clearErrors(int threadID) {
    try {
      System.err.println("Clearing region cache in threadId " + threadID);
      HConnection hm = table.getConnection();
      hm.clearRegionCache();
    } catch (Throwable e) {
      e.printStackTrace();
      return;
    }
  }


  @Override
  public boolean addLink(String dbid, Link a, boolean noinverse)
    throws Exception {

      String linkHead = combine(a.id1, a.link_type, a.id2);
      byte[] row = linkHead.getBytes();
      byte[] value = linkToBytes(a);

      // put data into table
      Put p = new Put(row);
      for (String cf : columnfamilies) {
        p.add(Bytes.toBytes(cf), Bytes.toBytes(""), value);
      }
      if (DEBUG) {
        System.out.println(threadname + ": addLink " +
            a.id1 + "." + a.link_type + "." + a.id2);
      }
      table.put(p);

      // sleep for some time
      if (currentphase == Phase.REQUEST &&
          Math.random() < sleeprate) {

        synchronized(lock) {
          if (counter < maxsleepingthreads) {
            ++counter;
            System.out.println(threadname + " goes to sleep. " +
                "Number of sleeping threads is: " + counter);
            try {
              lock.wait(sleeptime);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            --counter;
            System.out.println(threadname + " woke up. " +
                "Number of sleeping threads is: " + counter);
          }
        }
      }

    // make sure the new data is there, and value stored
    // in three column families are identical
    Get g = new Get(row);
    Result result = table.get(g);
    assertTrue(!result.isEmpty(), linkHead);
    for (String cf : columnfamilies) {
      byte[] tempvalue = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(""));
      assertTrue(Arrays.equals(value, tempvalue),
                 "rowid = " + linkHead +
                 "; column family = " + cf +
                 "; get value = " + bytesToString(value) +
                 "; expected value = " + bytesToString(tempvalue));
    }
    return true; // always pretend was added
  }


  @Override
  public boolean deleteLink(String dbid, long id1, long link_type, long id2,
                         boolean noinverse, boolean expunge)
    throws Exception {
    String linkHead = combine(id1, link_type, id2);
    byte[] row = linkHead.getBytes();

    // delete data from table
    Delete d = new Delete(row);
    if (DEBUG) {
      System.out.println(threadname + ": deleteLink " +
                         id1 + "." + link_type + "." + id2);
    }
    table.delete(d);

    // sleep for some time
    if (currentphase == Phase.REQUEST && Math.random() < sleeprate) {
      synchronized(lock) {
        if (counter < maxsleepingthreads) {
          ++counter;
          System.out.println(threadname + " goes to sleep. " +
                             "Number of sleeping threads is: " + counter);
          try {
            lock.wait(sleeptime);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          --counter;
          System.out.println(threadname + " woke up. " +
                             "Number of sleeping threads is: " + counter);
        }
      }
    }

    // check if data has been actually deleted
    Get g = new Get(row);
    Result result = table.get(g);
    assertTrue(result.isEmpty(), linkHead);
    return true; // always pretend was found
  }


  @Override
  public boolean updateLink(String dbid, Link a, boolean noinverse)
    throws Exception {
    addLink(dbid, a, noinverse);
    return true; // always pretend was updated
  }


  @Override
  public Link getLink(String dbid, long id1, long link_type, long id2)
    throws Exception {
    String linkHead = combine(id1, link_type, id2);
    byte[] row = linkHead.getBytes();

    // get data from table
    Get g = new Get(row);
    Result result = table.get(g);
    assertTrue(!result.isEmpty(), linkHead);

    // ensure values stored in three column families are identical
    byte[] value = null;
    for (String cf: columnfamilies) {
      byte[] tempvalue = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(""));
      if (value == null) value = tempvalue;
      else assertTrue(Arrays.equals(value, tempvalue),
          id1 + "." + link_type + "." + id2);
    }

    // return link
    Link a;
    if (value == null) a = null;
    else {
      a = bytesToLink(value);
      assertTrue(a.id1 == id1, linkHead);
      assertTrue(a.id2 == id2, linkHead);
      assertTrue(a.link_type == link_type, linkHead);
    }
    return a;
  }


  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type)
    throws Exception {
    throw new Exception("Don't use getLinkList in HBaseGeneralAtomicityTest");
  }


  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type,
                            long minTimestamp, long maxTimestamp,
                            int offset, int limit)
    throws Exception {
    throw new Exception("Don't use getLinkList in HBaseGeneralAtomicityTest");
  }


  // count the #links
  @Override
  public long countLinks(String dbid, long id1, long link_type)
    throws Exception {
    throw new Exception("Don't use countLinks in HBaseGeneralAtomicityTest");
  }

}



