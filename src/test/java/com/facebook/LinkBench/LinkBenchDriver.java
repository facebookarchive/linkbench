package com.facebook.LinkBench;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/*
 LinkBenchDriver class.
 First loads data using multi-threaded LinkBenchLoad class.
 Then does read and write requests of various types (addlink, deletelink,
 updatelink, getlink, countlinks, getlinklist) using multi-threaded
 LinkBenchRequest class.
 Config options are taken from config file passed as argument.
 */

public class LinkBenchDriver {
  
  public static final int EXIT_BADARGS = 1;
  
  private static String configFile = null;
  private static boolean doLoad = false;
  private static boolean doRequest = false;
  
  public static final int LOAD = 1;
  public static final int REQUEST = 2;

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

    if (!doLoad) {
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

    if (!doRequest) {
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
    processArgs(args);

    LinkBenchDriver d = new LinkBenchDriver(configFile);
    d.drive();
  }

  private static void printUsage(Options options) {
    //PrintWriter writer = new PrintWriter(System.err);
    HelpFormatter fmt = new HelpFormatter();
    fmt.printHelp("linkbench", options, true);
  }

  private static Options initializeOptions() {
    Options options = new Options();
    Option config = new Option("c", true,
                                "Linkbench config file");
    config.setArgName("file");
    options.addOption(config);
    
    options.addOption("l", false,
               "Execute loading stage of benchmark");
    options.addOption("r", false,
               "Execute request stage of benchmark");
    return options;
  }
  
  /**
   * Process command line arguments and set static variables
   * exits program if invalid arguments provided
   * @param options
   * @param args
   * @throws ParseException
   */
  private static void processArgs(String[] args)
              throws ParseException {
    Options options = initializeOptions();
    
    CommandLine cmd = null;
    try {
      CommandLineParser parser = new GnuParser();
      cmd = parser.parse( options, args);    
    } catch (ParseException ex) {
      // Use Apache CLI-provided messages
      System.err.println(ex.getMessage());
      printUsage(options);
      System.exit(EXIT_BADARGS);
    }
    
    /* 
     * Apache CLI validates arguments, so can now assume
     * all required options are present, etc
     */
    if (cmd.getArgs().length > 0) {
      System.err.print("Invalid trailing arguments:");
      for (String arg: cmd.getArgs()) {
        System.err.print(' ');
        System.err.print(arg);
      }
      System.err.println();
      printUsage(options);
      System.exit(EXIT_BADARGS);
    }   

    // Set static option variables
    doLoad = cmd.hasOption('l');
    doRequest = cmd.hasOption('r');
    
    configFile = cmd.getOptionValue('c');
    if (configFile == null) {
      // Try to find in usual location
      String linkBenchHome = ConfigUtil.findLinkBenchHome();
      if (linkBenchHome != null) {
        configFile = linkBenchHome + File.separator +
              "config" + File.separator + "LinkConfigMysql.properties";
      } else {
        System.err.println("Config file not specified through command "
            + "line argument and " + ConfigUtil.linkbenchHomeEnvVar
            + " environment variable not set to valid directory");
        printUsage(options);
        System.exit(EXIT_BADARGS);
      }
    }
    
    if (!(doLoad || doRequest)) {
      System.err.println("Did not select benchmark mode");
      printUsage(options);
      System.exit(EXIT_BADARGS);
    }
  }
}


