package com.facebook.LinkBench;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.facebook.LinkBench.LinkStore.LinkStoreOp;

/*
 * TODO: update to reflect changes
 * Muli-threaded loader for loading data into hbase.
 * The range from startid1 to maxid1 is chunked up into equal sized
 * disjoint ranges so that each loader can work on its range.
 * The #links generated for an id1 is random but based on probablities
 * given in nlinks_choices and odds_in_billion. The actual counts of
 * #links generated is tracked in nlinks_counts.
 */

public class LinkBenchLoad implements Runnable {

  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER); 

  private long randomid2max; // whether id2 should be generated randomly

  private long maxid1;   // max id1 to generate
  private long startid1; // id1 at which to start
  private int  loaderID; // ID for this loader
  private LinkStore store;// store interface (several possible implementations
                          // like mysql, hbase etc)
  private int datasize; // 'data' column size for one (id1, type, id2)
  private LinkBenchStats stats;
  private LinkBenchLatency latencyStats;
  long displayfreq;
  int maxsamples;

  Level debuglevel;
  String dbid;

  int nlinks_func; // distribution function for #links
  int nlinks_config; // any additional config to go with above the above func
  int nlinks_default; // default value of #links - expected to be 0 or 1

  
  // Counters for load statistics
  long sameShuffle;
  long diffShuffle;
  long linksloaded;
  
  // Random generators
  Random random_data;
  Random random_links = new Random(); // for #links
  private Random random_id2; // random number generator for id2 if needed
  
  /** 
   * special case for single hot row benchmark. If singleAssoc is set, 
   * then make this method not print any statistics message, all statistics
   * are collected at a higher layer. */
  boolean singleAssoc;

  private BlockingQueue<LoadChunk> chunk_q;

  private LoadProgress prog_tracker;


  /**
   * Convenience constructor
   * @param store2
   * @param props
   * @param latencyStats
   * @param loaderID
   * @param nloaders
   */
  public LinkBenchLoad(LinkStore store, Properties props,
      LinkBenchLatency latencyStats, int loaderID, boolean singleAssoc,
      int nloaders, LoadProgress prog_tracker) {
    this(store, props, latencyStats, loaderID, singleAssoc,
              new ArrayBlockingQueue<LoadChunk>(2), prog_tracker);
    
    // Just add a single chunk to the queue
    chunk_q.add(new LoadChunk(loaderID, startid1, maxid1));
    chunk_q.add(LoadChunk.SHUTDOWN);
  }

  
  public LinkBenchLoad(LinkStore store,
                       Properties props,
                       LinkBenchLatency latencyStats,
                       int loaderID,
                       boolean singleAssoc,
                       BlockingQueue<LoadChunk> chunk_q,
                       LoadProgress prog_tracker) throws LinkBenchConfigError {
    /*
     * Initialize fields from arguments
     */
    this.store = store;
    this.latencyStats = latencyStats;
    this.loaderID = loaderID;
    this.singleAssoc = singleAssoc;
    this.chunk_q = chunk_q;
    this.prog_tracker = prog_tracker;

    
    /*
     * Load settings from properties
     */
    maxid1 = Long.parseLong(props.getProperty("maxid1"));
    startid1 = Long.parseLong(props.getProperty("startid1"));

    // math functions may cause problems for id1 = 0. Start at 1.
    if (startid1 <= 0) {
      startid1 = 1;
    }

    debuglevel = ConfigUtil.getDebugLevel(props);
    datasize = Integer.parseInt(props.getProperty("datasize"));

    nlinks_func = Integer.parseInt(props.getProperty("nlinks_func"));
    nlinks_config = Integer.parseInt(props.getProperty("nlinks_config"));
    nlinks_default = Integer.parseInt(props.getProperty("nlinks_default"));
    if (nlinks_func == -2) {//real distribution has it own initialization
      try {
        //load statistical data into RealDistribution
        RealDistribution.loadOneShot(props);
      } catch (Exception e) {
        logger.error(e);
        System.exit(1);
      }
    }

    displayfreq = Long.parseLong(props.getProperty("displayfreq"));
    maxsamples = Integer.parseInt(props.getProperty("maxsamples"));
    
    dbid = props.getProperty("dbid");
    

    /*
     * Initialize statistics
     */
    linksloaded = 0;
    sameShuffle = 0;
    diffShuffle = 0;
    stats = new LinkBenchStats(loaderID, displayfreq, maxsamples);
    
    
    /*
     * Setup random number generators
     */
    // random number generator for id2 if needed
    randomid2max = Long.parseLong(props.getProperty("randomid2max"));
    random_id2 = (randomid2max > 0) ? (new Random()) : null;
    
    // generate data as a sequence of random characters from 'a' to 'd'
    random_data = new Random();
    
    // Random number generators for #links
    random_links = new Random();
  }

  public long getLinksLoaded() {
    return linksloaded;
  }

  // Gets the #links to generate for an id1 based on distribution specified
  // by nlinks_func, nlinks_config and nlinks_default.
  public static long getNlinks(long id1, long startid1, long maxid1,
                               int nlinks_func, int nlinks_config,
                               int nlinks_default) {
    long nlinks = nlinks_default; // start with nlinks_default
    long temp;

    switch(nlinks_func) {
    case -3 :
      return nlinks;
    case -2 :
      // real distribution
      nlinks = RealDistribution.getNlinks(id1, startid1, maxid1);
      break;

    case -1 :
      // Corresponds to function 1/x
      nlinks += (long)Math.ceil((double)maxid1/(double)id1);
      break;
    case 0 :
      // if id1 is multiple of nlinks_config, then add nlinks_config
      nlinks += (id1 % nlinks_config == 0 ? nlinks_config : 0);
      break;

    case 100 :
      // Corresponds to exponential distribution
      // If id1 is nlinks_config^k, then add
      // nlinks_config^k - nlinks_config^(k-1) more links
      long log = (long)Math.ceil(Math.log(id1)/Math.log(nlinks_config));
      temp = (long)Math.pow(nlinks_config, log);
      nlinks += (temp == id1 ?
                 (id1 - (long)Math.pow(nlinks_config, log - 1)) :
                 0);
      break;

    default:
      // if nlinks_func is 2 then
      //   if id1 is K * K, then add K * K - (K - 1) * (K - 1) more links.
      //   The idea is to give more #links to perfect squares. The larger
      //   the perfect square is, the more #links it will get.
      // Generalize the above for nlinks_func is n:
      //   if id1 is K^n, then add K^n - (K - 1)^n more links
      long nthroot = (long)Math.ceil(Math.pow(id1, (1.0)/nlinks_func));
      temp = (long)Math.pow(nthroot, nlinks_func);
      nlinks += (temp == id1 ?
                (id1 - (long)Math.pow(nthroot - 1, nlinks_func)) :
                0);
      break;
    }

    return nlinks;
  }

  @Override
  public void run() {

    int bulkLoadBatchSize = store.bulkLoadBatchSize();
    boolean bulkLoad = bulkLoadBatchSize > 0;
    ArrayList<Link> loadBuffer = null;
    if (bulkLoad) {
      loadBuffer = new ArrayList<Link>(bulkLoadBatchSize);
    }

    logger.info("Starting loader thread  #" + loaderID);
    
    while (true) {
      LoadChunk chunk;
      try {
        chunk = chunk_q.take();
      } catch (InterruptedException ie) {
        logger.warn("InterruptedException not expected, try again", ie);
        continue;
      }
      
      // Shutdown signal is received though special chunk type
      if (chunk.shutdown) {
        break;
      }

      // Load the link range specified in the chunk
      processChunk(chunk, bulkLoad, bulkLoadBatchSize, loadBuffer);
    }
    
    if (bulkLoad) {
      // Load any remaining links
      loadLinks(loadBuffer);
    }
    
    if (!singleAssoc) {
      logger.info(" Same shuffle = " + sameShuffle +
                         " Different shuffle = " + diffShuffle);
      stats.displayStatsAll();
    }
    
    store.close();
  }

  private void processChunk(LoadChunk chunk, boolean bulkLoad,
      int bulkLoadBatchSize, ArrayList<Link> loadBuffer) {
    if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
      logger.debug("Loader thread  #" + loaderID + " processing "
                  + chunk.toString());
    }

    Link link = null;
    if (!bulkLoad) {
      // When bulk-loading, need to have multiple link objects at a time
      // otherwise reuse object
      link = initLink();
    }
    
    long prevPercentPrinted = 0;
    for (long i = chunk.start; i < chunk.end; i += chunk.step) {
      long nlinks = 0;
      long id1;
      if (nlinks_func == -2) {
        long res[] = RealDistribution.getId1AndNLinks(i, startid1, maxid1);
        id1 = res[0];
        nlinks = res[1];
        if (id1 == i) {
          sameShuffle++;
        } else {
          diffShuffle++;
        }
      } else {
        id1 = i;
        nlinks = getNlinks(id1, startid1, maxid1,
                  nlinks_func, nlinks_config, nlinks_default);
      }
 
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("i = " + i + " id1 = " + id1 +
                           " nlinks = " + nlinks);
      }
 
      createOutLinks(link, loadBuffer, id1, nlinks, singleAssoc,
          bulkLoad, bulkLoadBatchSize);
 
      if (!singleAssoc) {
        long nloaded = (i - chunk.start) / chunk.step;
        if (bulkLoad) {
          nloaded -= loadBuffer.size();
        }
        long percent = 100 * nloaded/(chunk.size);
        if ((percent % 10 == 0) && (percent > prevPercentPrinted)) {
          logger.debug(chunk.toString() +  ": Percent done = " + percent);
          prevPercentPrinted = percent;
        }
      }
    }
    
    // Update progress and maybe print message
    prog_tracker.update(chunk.size);
  }

  private void createOutLinks(Link link, ArrayList<Link> loadBuffer,
      long id1, long nlinks, boolean singleAssoc, boolean bulkLoad,
      int bulkLoadBatchSize) {
    for (long j = 0; j < nlinks; j++) {
      if (bulkLoad) {
        // Can't reuse link object
        link = initLink();
      }
      constructLink(link, id1, j, singleAssoc);
      
      if (bulkLoad) {
        loadBuffer.add(link);
        if (loadBuffer.size() >= bulkLoadBatchSize) {
          loadLinks(loadBuffer);
        }
      } else {
        loadLink(link, j, nlinks, singleAssoc);
      }
    }
  }

  private Link initLink() {
    Link link = new Link();
    link.link_type = LinkStore.LINK_TYPE;
    link.id1_type = LinkStore.ID1_TYPE;
    link.id2_type = LinkStore.ID2_TYPE;
    link.visibility = LinkStore.VISIBILITY_DEFAULT;
    link.version = 0;
    link.data = new byte[datasize];
    link.time = System.currentTimeMillis();
    return link;
  }

  /**
   * Helper method to fill in link data
   * @param link this link is filled in.  Should have been initialized with
   *            initLink() earlier
   * @param outlink_ix the number of this link out of all outlinks from
   *                    id1
   * @param singleAssoc whether we are in singleAssoc mode
   */
  private void constructLink(Link link, long id1, long outlink_ix,
      boolean singleAssoc) {
    link.id1 = id1;

    // Using random number generator for id2 means we won't know
    // which id2s exist. So link id1 to
    // maxid1 + id1 + 1 thru maxid1 + id1 + nlinks(id1) UNLESS
    // config randomid2max is nonzero.
    if (singleAssoc) {
      link.id2 = 45; // some constant
    } else {
      link.id2 = (randomid2max == 0 ?
                 (maxid1 + id1 + outlink_ix) :
                 random_id2.nextInt((int)randomid2max));
      link.time = System.currentTimeMillis();
      // generate data as a sequence of random characters from 'a' to 'd'
      link.data = new byte[datasize];
      for (int k = 0; k < datasize; k++) {
        link.data[k] = (byte)('a' + Math.abs(random_data.nextInt()) % 4);
      }
    }

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("id2 chosen is " + link.id2);
    }
    
    link.time = System.currentTimeMillis();
  }

  /**
   * Load an individual link into the db.
   * 
   * If an error occurs during loading, this method will log it,
   *  add stats, and reset the connection.
   * @param link
   * @param outlink_ix
   * @param nlinks
   * @param singleAssoc
   */
  private void loadLink(Link link, long outlink_ix, long nlinks,
      boolean singleAssoc) {
    long timestart = 0;
    if (!singleAssoc) {
      timestart = System.nanoTime();
    }
    
    try {
      // no inverses for now
      store.addLink(dbid, link, true);
      linksloaded++;
  
      if (!singleAssoc && outlink_ix == nlinks - 1) {
        long timetaken = (System.nanoTime() - timestart);
  
        // convert to microseconds
        stats.addStats(LinkStoreOp.LOAD_LINK, timetaken/1000, false);
  
        latencyStats.recordLatency(loaderID, 
                      LinkStoreOp.LOAD_LINK, timetaken);
      }
  
    } catch (Throwable e){//Catch exception if any
        long endtime2 = System.nanoTime();
        long timetaken2 = (endtime2 - timestart)/1000;
        logger.error("Error: " + e.getMessage(), e);
        stats.addStats(LinkStoreOp.LOAD_LINK, timetaken2, true);
        store.clearErrors(loaderID);
    }
  }

  private void loadLinks(ArrayList<Link> loadBuffer) {
    long timestart = System.nanoTime();
    
    try {
      // no inverses for now
      int nlinks = loadBuffer.size();
      store.addBulkLinks(dbid, loadBuffer, true);
      linksloaded += nlinks;
      loadBuffer.clear();
  
      long timetaken = (System.nanoTime() - timestart);
  
      // convert to microseconds
      stats.addStats(LinkStoreOp.LOAD_LINKS_BULK, timetaken/1000, false);
      stats.addStats(LinkStoreOp.LOAD_LINKS_BULK_NLINKS, linksloaded, false);
  
      latencyStats.recordLatency(loaderID, LinkStoreOp.LOAD_LINKS_BULK,
                                                             timetaken);
    } catch (Throwable e){//Catch exception if any
        long endtime2 = System.nanoTime();
        long timetaken2 = (endtime2 - timestart)/1000;
        logger.error("Error: " + e.getMessage(), e);
        stats.addStats(LinkStoreOp.LOAD_LINKS_BULK, timetaken2, true);
        store.clearErrors(loaderID);
    }
  }
  
  /**
   * Represents a portion of the id space, starting with
   * start, going up until end (non-inclusive) with step size
   * step
   *
   */
  public static class LoadChunk {
    public static LoadChunk SHUTDOWN = new LoadChunk(true,
                                              0, 0, 0, 1);

    public LoadChunk(long id, long start, long end) {
      this(false, id, start, end, 1);
    }
    public LoadChunk(boolean shutdown,
                      long id, long start, long end, long step) {
      super();
      this.shutdown = shutdown;
      this.id = id;
      this.start = start;
      this.end = end;
      this.step = step;
      this.size = (end - start) / step;
    }
    public final boolean shutdown;
    public final long id;
    public final long start;
    public final long end;
    public final long step;
    public final long size;
    
    public String toString() {
      if (shutdown) {
        return "chunk SHUTDOWN";
      }
      String range;
      if (step == 1) {
        range = "[" + start + ":" + end + "]";
      } else {
        range = "[" + start + ":" + step + ":" + end + "]";
      }
      return "chunk " + id + range;
    }
  }
  public static class LoadProgress {
    public LoadProgress(Logger progressLogger, long ntotal) {
      super();
      this.progressLogger = progressLogger;
      this.ntotal = ntotal;
      this.nloaded = new AtomicLong();
    }
    private final Logger progressLogger;
    private final AtomicLong nloaded; // progress
    private final long ntotal; // goal
    
    public void update(long inc) {
      // TODO: check semantics
      long curr = nloaded.addAndGet(inc);
      long prev = curr - inc;
      
      // TODO: temporary
      long report_int = (1024);
      if ((curr / report_int) > (prev / report_int)
          || curr == ntotal) {
        double percentage = (curr / (double)ntotal) * 100.0;
        progressLogger.info(
            String.format("%d/%d id1s loaded: %.1f%% complete: ",
                          curr, ntotal, percentage));
      }
    }
  }
}
