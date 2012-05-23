package com.facebook.LinkBench;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Properties;
import java.util.List;
import java.util.LinkedList;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/*
 LinkBenchDriver class.
 First loads data using multi-threaded LinkBenchLoad class.
 Then does read and write requests of various types (addlink, deletelink,
 updatelink, getlink, countlinks, getlinklist) using multi-threaded
 LinkBenchRequest class.
 Config options are taken from config file passed as argument.
 */

public class LinkBenchDriver {
  public static final int LOAD = 1;
  public static final int REQUEST = 2;

  private static int mode = 0;

  private Properties props;
  private String store;

  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};

  LinkBenchDriver(String configfile)
    throws java.io.FileNotFoundException, IOException {
    // which link store to use
    props = new Properties();
    props.load(new FileInputStream(configfile));
    store = null;
  }

  // generate an instance of LinkStore
  private LinkStore initStore(int currentphase, int threadid)
    throws IOException {

    LinkStore newstore = null;

    if (store == null) {
      store = props.getProperty("store");
      System.out.println(store);
    }

    // The property "store" defines the class name that will be used to
    // store data in a database. The folowing class names are pre-packaged
    // for easy access:
    //   LinkStoreMysql :  run benchmark on  mySQL
    //   LinkStoreHBase :  run benchmark on  HBase
    //   LinkStoreHBaseGeneralAtomicityTesting : atomicity testing on HBase.
    //   LinkStoreTaoAtomicityTesting:  atomicity testing for Facebook's HBase
    //
    Class<?> clazz = null;
    try {
      clazz = getClassByName(store);
    } catch (java.lang.ClassNotFoundException nfe) {
      throw new IOException("Cound not find class for " + store);
    }
    newstore = (LinkStore)newInstance(clazz);
    if (clazz == null) {
      System.out.println("Unknown data store " + store);
      System.exit(1);
      return null;
    }
    newstore.initialize(props, currentphase, threadid);

    return newstore;
  }

  void load() throws IOException, InterruptedException, Throwable {

    if (mode == REQUEST) {
      System.out.println("Skipping load data per the cmdline arg");
      return;
    }

    // load data

    int nloaders = Integer.parseInt(props.getProperty("loaders"));
    List<Runnable> loaders = new LinkedList<Runnable>();
    LinkBenchLatency latencyStats = new LinkBenchLatency(nloaders);

    // start loaders
    System.out.println("Starting loaders " + nloaders);
    for (int i = 0; i < nloaders; i++) {
      LinkStore store = initStore(LOAD, i);
      Runnable l = new LinkBenchLoad(store, props, latencyStats, i, nloaders);
      loaders.add(l);
    }

    // run loaders
    long loadtime = concurrentExec(loaders);

    // compute total #links loaded
    long maxid1 = Long.parseLong(props.getProperty("maxid1"));
    long startid1 = Long.parseLong(props.getProperty("startid1"));
    int nlinks_default = Integer.parseInt(props.getProperty("nlinks_default"));

    long expectedlinks = (1 + nlinks_default) * (maxid1 - startid1);

    long actuallinks = 0;
    for (final Runnable r:loaders) {
      LinkBenchLoad l = (LinkBenchLoad)r;
      actuallinks += l.getLinksLoaded();
    }

    latencyStats.displayLatencyStats();
    System.out.println("LOAD PHASE COMPLETED. Expected to load " +
                       expectedlinks + " links. " +
                       actuallinks + " loaded in " + (loadtime/1000) + " seconds." +
                       "Links/second = " + ((1000*actuallinks)/loadtime));

  }

  void sendrequests() throws IOException, InterruptedException, Throwable {

    if (mode == LOAD) {
      System.out.println("Skipping request phase per the cmdline arg");
      return;
    }

    // config info for requests
    int nrequesters = Integer.parseInt(props.getProperty("requesters"));
    if (nrequesters == 0) {
      System.out.println("NO REQUEST PHASE CONFIGURED. ");
      return;
    }
    LinkBenchLatency latencyStats = new LinkBenchLatency(nrequesters);
    List<Runnable> requesters = new LinkedList<Runnable>();

    // create requesters
    for (int i = 0; i < nrequesters; i++) {
      LinkStore store = initStore(REQUEST, i);
      Runnable l = new LinkBenchRequest(store, props, latencyStats,
                                        i, nrequesters);
      requesters.add(l);
    }

    // run requesters
    long requesttime = concurrentExec(requesters);

    long requestsdone = 0;
    // wait for requesters
    for (int j = 0; j < nrequesters; j++) {
      requestsdone += ((LinkBenchRequest)requesters.get(j)).getRequestsDone();
    }

    latencyStats.displayLatencyStats();
    System.out.println("REQUEST PHASE COMPLETED. " + requestsdone +
                       " requests done in " + (requesttime/1000) + " seconds." +
                       "Requests/second = " + (1000*requestsdone)/requesttime);
  }

  /**
   * Start all runnables at the same time. Then block till all
   * tasks are completed. Returns the elapsed time (in millisec)
   * since the start of the first task to the completion of all tasks.
   */
  static long concurrentExec(final List<Runnable> tasks)
      throws Throwable {
    final CountDownLatch startSignal = new CountDownLatch(tasks.size());
    final CountDownLatch doneSignal = new CountDownLatch(tasks.size());
    final AtomicLong startTime = new AtomicLong(0);
    for (final Runnable task : tasks) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            startSignal.countDown();
            startSignal.await();
            long now = System.currentTimeMillis();
            startTime.compareAndSet(0, now);
            task.run();
          } catch (Throwable e) {
            e.printStackTrace();
          }
          doneSignal.countDown();
        }
      }).start();
    }
    doneSignal.await(); // wait for all threads to finish
    long endTime = System.currentTimeMillis();
    return endTime - startTime.get();
  }

  void drive() throws IOException, InterruptedException, Throwable {
    load();
    sendrequests();
  }

  /**
   * Load a class by name.
   * @param name the class name.
   * @return the class object.
   * @throws ClassNotFoundException if the class is not found.
   */
  public Class<?> getClassByName(String name) throws ClassNotFoundException {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return Class.forName(name, true, classLoader);
  }

  /** Create an object for the given class and initialize it from conf
   *
   * @param theClass class of which an object is created
   * @param conf Configuration
   * @return a new object
   */
  public static <T> T newInstance(Class<T> theClass) {
    T result;
    try {
      Constructor<T> meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
      meth.setAccessible(true);
      result = meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static void main(String[] args)
    throws IOException, InterruptedException, Throwable {

    if (args.length < 2) {
      System.out.println("Args : LinkBenchDriver configfile mode.");
      System.out.println("Use mode=1 for load, mode=2 for requests, mode=3 for both");
      return;
    }

    mode = Integer.parseInt(args[1]);
    if (mode < 1 || mode > 3) {
      System.out.println("Invalid mode. Mode should be 1, 2 or 3");
      return;
    }

    LinkBenchDriver d = new LinkBenchDriver(args[0]);
    d.drive();

  }
}


