package com.facebook.LinkBench;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.facebook.LinkBench.LinkBenchLoad.LoadChunk;
import com.facebook.LinkBench.LinkBenchLoad.LoadProgress;
import com.facebook.LinkBench.LinkBenchRequest.RequestProgress;
import com.facebook.LinkBench.util.ClassLoadUtil;

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
  
  /* Command line arguments */
  private static String configFile = null;
  private static Properties cmdLineProps = null;
  private static String logFile = null;
  private static boolean doLoad = false;
  private static boolean doRequest = false;
  
  private Properties props;
  
  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER); 

  LinkBenchDriver(String configfile, Properties
                  overrideProps, String logFile)
    throws java.io.FileNotFoundException, IOException, LinkBenchConfigError {
    // which link store to use
    props = new Properties();
    props.load(new FileInputStream(configfile));
    for (String key: overrideProps.stringPropertyNames()) {
      props.setProperty(key, overrideProps.getProperty(key));
    }
    
    ConfigUtil.setupLogging(props, logFile);
  }

  private static class Stores {
    final LinkStore linkStore;
    final NodeStore nodeStore;
    public Stores(LinkStore linkStore, NodeStore nodeStore) {
      super();
      this.linkStore = linkStore;
      this.nodeStore = nodeStore;
    }
  }

  // generate instances of LinkStore and NodeStore
  private Stores initStores(Phase currentphase, int threadid)
    throws Exception {
    LinkStore linkStore = createLinkStore(currentphase, threadid);
    NodeStore nodeStore = createNodeStore(currentphase, threadid, linkStore);
    
    return new Stores(linkStore, nodeStore);
  }

  private LinkStore createLinkStore(Phase currentphase, int threadid)
      throws Exception, IOException {
    // The property "linkstore" defines the class name that will be used to
    // store data in a database. The folowing class names are pre-packaged
    // for easy access:
    //   LinkStoreMysql :  run benchmark on  mySQL
    //   LinkStoreHBaseGeneralAtomicityTesting : atomicity testing on HBase.
    
    String linkStoreClassName = props.getProperty(Config.LINKSTORE_CLASS);
    if (linkStoreClassName == null) {
      throw new Exception("Config key " + Config.LINKSTORE_CLASS + 
                          " must be defined");
    }
    logger.info("Using LinkStore implementation: " + linkStoreClassName);
    
    LinkStore linkStore;
    try {
      linkStore = ClassLoadUtil.newInstance(linkStoreClassName, 
                                            LinkStore.class);
    } catch (ClassNotFoundException nfe) {
      throw new IOException("Cound not find class for " + linkStoreClassName);
    }
  
    try {
      linkStore.initialize(props, currentphase, threadid);
    } catch (Exception e) {
      System.err.println("Error while initializing data store:");
      e.printStackTrace();
      System.exit(1);
    }
    return linkStore;
  }

  /**
   * @param linkStore a LinkStore instance to be reused if it turns out
   * that linkStore and nodeStore classes are same
   * @return
   * @throws Exception
   * @throws IOException
   */
  private NodeStore createNodeStore(Phase currentPhase, int threadId, 
                                        LinkStore linkStore) throws Exception,
      IOException {
    String nodeStoreClassName = props.getProperty(Config.NODESTORE_CLASS);
    if (nodeStoreClassName == null) {
      logger.info("No NodeStore implementation provided");
    } else {
      logger.info("Using NodeStore implementation: " + nodeStoreClassName);
    }
    
    if (linkStore != null && linkStore.getClass().getName().equals(
                                                nodeStoreClassName)) {
      // Same class, reuse object
      if (!NodeStore.class.isAssignableFrom(linkStore.getClass())) {
        throw new Exception("Specified NodeStore class " + nodeStoreClassName 
                          + " is not a subclass of NodeStore");
      }
      return (NodeStore)linkStore;
    } else {
      NodeStore nodeStore;
      try {
        nodeStore = ClassLoadUtil.newInstance(nodeStoreClassName,
                                                            NodeStore.class);
        nodeStore.initialize(props, currentPhase, threadId);
        return nodeStore;
      } catch (java.lang.ClassNotFoundException nfe) {
        throw new IOException("Cound not find class for " + nodeStoreClassName);
      }
    }
  }

  void load() throws IOException, InterruptedException, Throwable {

    if (!doLoad) {
      logger.info("Skipping load data per the cmdline arg");
      return;
    }

    // load data
    int nLinkLoaders = Integer.parseInt(props.getProperty(Config.NUM_LOADERS));
    

    boolean bulkLoad = true;
    BlockingQueue<LoadChunk> chunk_q = new LinkedBlockingQueue<LoadChunk>();
    
    // max id1 to generate
    long maxid1 = Long.parseLong(props.getProperty(Config.MAX_ID));
    // id1 at which to start
    long startid1 = Long.parseLong(props.getProperty(Config.MIN_ID));
    
    // Create loaders
    logger.info("Starting loaders " + nLinkLoaders);
    logger.debug("Bulk Load setting: " + bulkLoad);
    
    Random masterRandom = createMasterRNG(props, Config.LOAD_RANDOM_SEED);
    

    boolean genNodes = Boolean.parseBoolean(props.getProperty(
                                            Config.GENERATE_NODES));
    int nTotalLoaders = genNodes ? nLinkLoaders + 1 : nLinkLoaders;
    
    LinkBenchLatency latencyStats = new LinkBenchLatency(nTotalLoaders);
    List<Runnable> loaders = new ArrayList<Runnable>(nTotalLoaders);
    
    LoadProgress loadTracker = new LoadProgress(logger, maxid1 - startid1); 
    for (int i = 0; i < nLinkLoaders; i++) {
      LinkStore linkStore = createLinkStore(Phase.LOAD, i);
      
      bulkLoad = bulkLoad && linkStore.bulkLoadBatchSize() > 0;
      LinkBenchLoad l = new LinkBenchLoad(linkStore, props, latencyStats, i,
                          maxid1 == startid1 + 1, chunk_q, loadTracker);
      loaders.add(l);
    }
    
    if (genNodes) {
      logger.info("Will generate graph nodes during loading");
      int loaderId = nTotalLoaders - 1;
      NodeStore nodeStore = createNodeStore(Phase.LOAD, loaderId, null);
      Random rng = new Random(masterRandom.nextLong());
      loaders.add(new NodeLoader(props, logger, nodeStore, rng,
          latencyStats, loaderId));
    }
    
    enqueueLoadWork(chunk_q, startid1, maxid1, nLinkLoaders, 
                    new Random(masterRandom.nextLong()));

    // run loaders
    loadTracker.startTimer();
    long loadtime = concurrentExec(loaders);

    // compute total #links loaded
    int nlinks_default = Integer.parseInt(props.getProperty(
                                            Config.NLINKS_DEFAULT));

    long expectedlinks = (1 + nlinks_default) * (maxid1 - startid1);
    long expectedNodes = maxid1 - startid1;
    long actuallinks = 0;
    long actualNodes = 0;
    for (final Runnable l:loaders) {
      if (l instanceof LinkBenchLoad) {
        actuallinks += ((LinkBenchLoad)l).getLinksLoaded();
      } else {
        assert(l instanceof NodeLoader);
        actualNodes += ((NodeLoader)l).getNodesLoaded();
      }
    }

    latencyStats.displayLatencyStats();
    logger.info("LOAD PHASE COMPLETED. Loaded " + actualNodes + "/" + 
          expectedNodes + " nodes. " +
          "Expected to load " + expectedlinks + " links. " +
           actuallinks + " links loaded in " + (loadtime/1000) 
           + " seconds." + "Links/second = " + ((1000*actuallinks)/loadtime));

  }

  /**
   * Create a new random number generated, optionally seeded to a known
   * value from the config file.  If seed value not provided, a seed
   * is chosen.  In either case the seed is logged for later reproducibility.
   * @param props
   * @param configKey config key for the seed value
   * @return
   */
  private Random createMasterRNG(Properties props, String configKey) {
    long seed;
    if (props.containsKey(configKey)) {
      seed = Long.parseLong(props.getProperty(configKey));
      logger.info("Using configured random seed " + configKey + "=" + seed);
    } else {
      seed = System.nanoTime() ^ (long)configKey.hashCode();
      logger.info("Using random seed " + seed + " since " + configKey 
          + " not specified");
    }
    
    SecureRandom masterRandom;
    try {
      masterRandom = SecureRandom.getInstance("SHA1PRNG");
    } catch (NoSuchAlgorithmException e) {
      logger.warn("SHA1PRNG not available, defaulting to default SecureRandom" +
      		" implementation");
      masterRandom = new SecureRandom();
    }
    masterRandom.setSeed(ByteBuffer.allocate(8).putLong(seed).array());
    
    // Can be used to check that rng is behaving as expected
    logger.debug("First number generated by master " + configKey + 
                 ": " + masterRandom.nextLong());
    return masterRandom;
  }

  private void enqueueLoadWork(BlockingQueue<LoadChunk> chunk_q, long startid1,
      long maxid1, int nloaders, Random rng) {
    // Enqueue work chunks.  Do it in reverse order as a heuristic to improve
    // load balancing, since queue is FIFO and later chunks tend to be larger
    
    int chunkSize = Integer.parseInt(
                      props.getProperty(Config.LOADER_CHUNK_SIZE, "2048"));
    long chunk_num = 0;
    ArrayList<LoadChunk> stack = new ArrayList<LoadChunk>();
    for (long id1 = startid1; id1 < maxid1; id1 += chunkSize) {
      stack.add(new LoadChunk(chunk_num, id1, 
                    Math.min(id1 + chunkSize, maxid1), rng));
      chunk_num++;
    }
    
    for (int i = stack.size() - 1; i >= 0; i--) {
      chunk_q.add(stack.get(i));
    }
    
    for (int i = 0; i < nloaders; i++) {
      // Add a shutdown signal for each loader
      chunk_q.add(LoadChunk.SHUTDOWN);
    }
  }

  void sendrequests() throws IOException, InterruptedException, Throwable {

    if (!doRequest) {
      logger.info("Skipping request phase per the cmdline arg");
      return;
    }

    // config info for requests
    int nrequesters = Integer.parseInt(props.getProperty(
                                            Config.NUM_REQUESTERS));
    if (nrequesters == 0) {
      logger.info("NO REQUEST PHASE CONFIGURED. ");
      return;
    }
    LinkBenchLatency latencyStats = new LinkBenchLatency(nrequesters);
    List<LinkBenchRequest> requesters = new LinkedList<LinkBenchRequest>();

    RequestProgress progress = LinkBenchRequest.createProgress(logger, props);
    
    Random masterRandom = createMasterRNG(props, Config.REQUEST_RANDOM_SEED);
    
    // create requesters
    for (int i = 0; i < nrequesters; i++) {
      Stores stores = initStores(Phase.REQUEST, i);
      LinkBenchRequest l = new LinkBenchRequest(stores.linkStore,
              stores.nodeStore, props, latencyStats, progress, 
              new Random(masterRandom.nextLong()), i, nrequesters);
      requesters.add(l);
    }

    progress.startTimer();
    // run requesters
    long requesttime = concurrentExec(requesters);

    long requestsdone = 0;
    int abortedRequesters = 0;
    // wait for requesters
    for (LinkBenchRequest requester: requesters) {
      requestsdone += requester.getRequestsDone();
      if (requester.didAbort()) {
        abortedRequesters++;
      }
    }

    latencyStats.displayLatencyStats();
    logger.info("REQUEST PHASE COMPLETED. " + requestsdone +
                       " requests done in " + (requesttime/1000) + " seconds." +
                       "Requests/second = " + (1000*requestsdone)/requesttime);
    if (abortedRequesters > 0) {
      logger.error(String.format("Benchmark did not complete cleanly: %d/%d " +
      		"request threads aborted.  See error log entries for details.",
      		abortedRequesters, nrequesters));  
    }
  }

  /**
   * Start all runnables at the same time. Then block till all
   * tasks are completed. Returns the elapsed time (in millisec)
   * since the start of the first task to the completion of all tasks.
   */
  static long concurrentExec(final List<? extends Runnable> tasks)
      throws Throwable {
    final CountDownLatch startSignal = new CountDownLatch(tasks.size());
    final CountDownLatch doneSignal = new CountDownLatch(tasks.size());
    final AtomicLong startTime = new AtomicLong(0);
    for (final Runnable task : tasks) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          /*
           * Run a task.  If an uncaught exception occurs, bail
           * out of the benchmark immediately, since any results
           * of the benchmark will no longer be valid anyway
           */
          try {
            startSignal.countDown();
            startSignal.await();
            long now = System.currentTimeMillis();
            startTime.compareAndSet(0, now);
            task.run();
          } catch (Throwable e) {
            Logger threadLog = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
            threadLog.error("Unrecoverable exception in" +
            		" worker thread:", e);
            System.exit(1);
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

  public static void main(String[] args)
    throws IOException, InterruptedException, Throwable {
    processArgs(args);
    
    LinkBenchDriver d = new LinkBenchDriver(configFile, 
                                cmdLineProps, logFile);
    d.drive();
  }

  private static void printUsage(Options options) {
    //PrintWriter writer = new PrintWriter(System.err);
    HelpFormatter fmt = new HelpFormatter();
    fmt.printHelp("linkbench", options, true);
  }

  private static Options initializeOptions() {
    Options options = new Options();
    Option config = new Option("c", true, "Linkbench config file");
    config.setArgName("file");
    options.addOption(config);
    
    Option log = new Option("L", true, "Log to this file");
    log.setArgName("file");
    options.addOption(log);
    
    options.addOption("l", false,
               "Execute loading stage of benchmark");
    options.addOption("r", false,
               "Execute request stage of benchmark");
    
    // Java-style properties to override config file
    // -Dkey=value
    Option property = new Option("D", "Override a config setting");
    property.setArgs(2);
    property.setArgName("property=value");
    property.setValueSeparator('=');
    options.addOption(property);
    
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
    
    logFile = cmd.getOptionValue('L'); // May be null
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
    
    cmdLineProps = cmd.getOptionProperties("D");
    
    if (!(doLoad || doRequest)) {
      System.err.println("Did not select benchmark mode");
      printUsage(options);
      System.exit(EXIT_BADARGS);
    }
  }
}


