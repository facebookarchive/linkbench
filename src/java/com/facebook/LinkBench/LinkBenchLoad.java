package com.facebook.LinkBench;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.facebook.LinkBench.distributions.ID2Chooser;
import com.facebook.LinkBench.distributions.LogNormalDistribution;
import com.facebook.LinkBench.generators.DataGenerator;
import com.facebook.LinkBench.stats.LatencyStats;
import com.facebook.LinkBench.stats.SampledStats;
import com.facebook.LinkBench.util.ClassLoadUtil;


/*
 * Multi-threaded loader for loading graph edges (but not nodes) into
 * LinkStore. The range from startid1 to maxid1 is chunked up into equal sized
 * disjoint ranges.  These are then enqueued for processing by a number
 * of loader threads to be loaded in parallel. The #links generated for 
 * an id1 is based on the configured distribution.  The # of link types,
 * and link payload data is also controlled by the configuration file.
 *  The actual counts of #links generated is tracked in nlinks_counts.
 */

public class LinkBenchLoad implements Runnable {

  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER); 

  private long maxid1;   // max id1 to generate
  private long startid1; // id1 at which to start
  private int  loaderID; // ID for this loader
  private LinkStore store;// store interface (several possible implementations
                          // like mysql, hbase etc)

  private final LogNormalDistribution linkDataSize;
  private final DataGenerator linkDataGen; // Generate link data
  private SampledStats stats;
  private LatencyStats latencyStats;

  Level debuglevel;
  String dbid;
  
  private ID2Chooser id2chooser;

  // Counters for load statistics
  long sameShuffle;
  long diffShuffle;
  long linksloaded;
  
  /** 
   * special case for single hot row benchmark. If singleAssoc is set, 
   * then make this method not print any statistics message, all statistics
   * are collected at a higher layer. */
  boolean singleAssoc;

  private BlockingQueue<LoadChunk> chunk_q;

  // Track last time stats were updated in ms
  private long lastDisplayTime;
  // How often stats should be reported
  private final long displayFreq_ms;

  private LoadProgress prog_tracker;

  private Properties props;

  /**
   * Convenience constructor
   * @param store2
   * @param props
   * @param latencyStats
   * @param loaderID
   * @param nloaders
   */
  public LinkBenchLoad(LinkStore store, Properties props,
      LatencyStats latencyStats, PrintStream csvStreamOut,
      int loaderID, boolean singleAssoc,
      int nloaders, LoadProgress prog_tracker, Random rng) {
    this(store, props, latencyStats, csvStreamOut, loaderID, singleAssoc,
              new ArrayBlockingQueue<LoadChunk>(2), prog_tracker);
    
    // Just add a single chunk to the queue
    chunk_q.add(new LoadChunk(loaderID, startid1, maxid1, rng));
    chunk_q.add(LoadChunk.SHUTDOWN);
  }

  
  public LinkBenchLoad(LinkStore linkStore,
                       Properties props,
                       LatencyStats latencyStats,
                       PrintStream csvStreamOut,
                       int loaderID,
                       boolean singleAssoc,
                       BlockingQueue<LoadChunk> chunk_q,
                       LoadProgress prog_tracker) throws LinkBenchConfigError {
    /*
     * Initialize fields from arguments
     */
    this.store = linkStore;
    this.props = props;
    this.latencyStats = latencyStats;
    this.loaderID = loaderID;
    this.singleAssoc = singleAssoc;
    this.chunk_q = chunk_q;
    this.prog_tracker = prog_tracker;
    
    
    /*
     * Load settings from properties
     */
    maxid1 = ConfigUtil.getLong(props, Config.MAX_ID);
    startid1 = ConfigUtil.getLong(props, Config.MIN_ID);
    
    // math functions may cause problems for id1 = 0. Start at 1.
    if (startid1 <= 0) {
      throw new LinkBenchConfigError("startid1 must be >= 1");
    }

    debuglevel = ConfigUtil.getDebugLevel(props);
    
    double medianLinkDataSize = ConfigUtil.getDouble(props,
                                              Config.LINK_DATASIZE);
    linkDataSize = new LogNormalDistribution();
    linkDataSize.init(0, LinkStore.MAX_LINK_DATA, medianLinkDataSize,
                                         Config.LINK_DATASIZE_SIGMA);
    
    try {
      linkDataGen = ClassLoadUtil.newInstance(
          ConfigUtil.getPropertyRequired(props, Config.LINK_ADD_DATAGEN),
          DataGenerator.class);
      linkDataGen.init(props, Config.LINK_ADD_DATAGEN_PREFIX);
    } catch (ClassNotFoundException ex) {
      logger.error(ex);
      throw new LinkBenchConfigError("Error loading data generator class: " 
            + ex.getMessage());
    }
    
    displayFreq_ms = ConfigUtil.getLong(props, Config.DISPLAY_FREQ) * 1000;
    int maxsamples = ConfigUtil.getInt(props, Config.MAX_STAT_SAMPLES);
    
    dbid = ConfigUtil.getPropertyRequired(props, Config.DBID);
        
    /*
     * Initialize statistics
     */
    linksloaded = 0;
    sameShuffle = 0;
    diffShuffle = 0;
    stats = new SampledStats(loaderID, maxsamples, csvStreamOut);
    
    id2chooser = new ID2Chooser(props, startid1, maxid1, 1, 1);
  }

  public long getLinksLoaded() {
    return linksloaded;
  }

  @Override
  public void run() {
    try {
      this.store.initialize(props, Phase.LOAD, loaderID);
    } catch (Exception e) {
      logger.error("Error while initializing store", e);
      throw new RuntimeException(e);
    }
    
    int bulkLoadBatchSize = store.bulkLoadBatchSize();
    boolean bulkLoad = bulkLoadBatchSize > 0;
    ArrayList<Link> loadBuffer = null;
    ArrayList<LinkCount> countLoadBuffer = null;
    if (bulkLoad) {
      loadBuffer = new ArrayList<Link>(bulkLoadBatchSize);
      countLoadBuffer = new ArrayList<LinkCount>(bulkLoadBatchSize);
    }

    logger.info("Starting loader thread  #" + loaderID + " loading links");
    lastDisplayTime = System.currentTimeMillis();
    
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
      processChunk(chunk, bulkLoad, bulkLoadBatchSize,
                    loadBuffer, countLoadBuffer);
    }
    
    if (bulkLoad) {
      // Load any remaining links or counts
      loadLinks(loadBuffer);
      loadCounts(countLoadBuffer);
    }
    
    if (!singleAssoc) {
      logger.debug(" Same shuffle = " + sameShuffle +
                         " Different shuffle = " + diffShuffle);
      displayStats(lastDisplayTime, bulkLoad);
    }
    
    store.close();
  }


  private void displayStats(long startTime, boolean bulkLoad) {
    long endTime = System.currentTimeMillis();
    if (bulkLoad) {
      stats.displayStats(startTime, endTime, 
          Arrays.asList(LinkBenchOp.LOAD_LINKS_BULK,
          LinkBenchOp.LOAD_COUNTS_BULK, LinkBenchOp.LOAD_LINKS_BULK_NLINKS,
          LinkBenchOp.LOAD_COUNTS_BULK_NLINKS));
    } else {
      stats.displayStats(startTime, endTime,
                         Arrays.asList(LinkBenchOp.LOAD_LINK));
    }
  }

  private void processChunk(LoadChunk chunk, boolean bulkLoad,
      int bulkLoadBatchSize, ArrayList<Link> loadBuffer,
      ArrayList<LinkCount> countLoadBuffer) {
    if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
      logger.debug("Loader thread  #" + loaderID + " processing "
                  + chunk.toString());
    }
    
    // Counter for total number of links loaded in chunk;
    long links_in_chunk = 0;

    Link link = null;
    if (!bulkLoad) {
      // When bulk-loading, need to have multiple link objects at a time
      // otherwise reuse object
      link = initLink();
    }
    
    long prevPercentPrinted = 0;
    for (long id1 = chunk.start; id1 < chunk.end; id1 += chunk.step) {
      long added_links= createOutLinks(chunk.rng, link, loadBuffer, countLoadBuffer,
          id1, singleAssoc, bulkLoad, bulkLoadBatchSize);
      links_in_chunk += added_links;
 
      if (!singleAssoc) {
        long nloaded = (id1 - chunk.start) / chunk.step;
        if (bulkLoad) {
          nloaded -= loadBuffer.size();
        }
        long percent = 100 * nloaded/(chunk.size);
        if ((percent % 10 == 0) && (percent > prevPercentPrinted)) {
          logger.debug(chunk.toString() +  ": Percent done = " + percent);
          prevPercentPrinted = percent;
        }
      }
      
      // Check if stats should be flushed and reset
      long now = System.currentTimeMillis();
      if (lastDisplayTime + displayFreq_ms <= now) {
        displayStats(lastDisplayTime, bulkLoad);
        stats.resetSamples();
        lastDisplayTime = now;
      }
    }
    
    // Update progress and maybe print message
    prog_tracker.update(chunk.size, links_in_chunk);
  }

  /**
   * Create the out links for a given id1
   * @param link
   * @param loadBuffer
   * @param id1
   * @param singleAssoc
   * @param bulkLoad
   * @param bulkLoadBatchSize
   * @return total number of links added
   */
  private long createOutLinks(Random rng,
      Link link, ArrayList<Link> loadBuffer,
      ArrayList<LinkCount> countLoadBuffer,
      long id1, boolean singleAssoc, boolean bulkLoad,
      int bulkLoadBatchSize) {
    Map<Long, LinkCount> linkTypeCounts = null;
    if (bulkLoad) {
      linkTypeCounts = new HashMap<Long, LinkCount>();
    }
    long nlinks_total = 0;
    
    for (long link_type: id2chooser.getLinkTypes()) {
      long nlinks = id2chooser.calcLinkCount(id1, link_type);
      nlinks_total += nlinks;
      if (id2chooser.sameShuffle) {
        sameShuffle++;
      } else {
        diffShuffle++;
      }
     
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("id1 = " + id1 + " link_type = " + link_type +
                           " nlinks = " + nlinks);
      }
      for (long j = 0; j < nlinks; j++) {
        if (bulkLoad) {
          // Can't reuse link object
          link = initLink();
        }
        constructLink(rng, link, id1, link_type, j, singleAssoc);
        
        if (bulkLoad) {
          loadBuffer.add(link);
          if (loadBuffer.size() >= bulkLoadBatchSize) {
            loadLinks(loadBuffer);
          }
          
          // Update link counts for this type
          LinkCount count = linkTypeCounts.get(link.link_type); 
          if (count == null) {
            count = new LinkCount(id1,link.id1_type, link.link_type,
                                  link.time, link.version, 1);
            linkTypeCounts.put(link.link_type, count);
          } else {
            count.count++;
            count.time = Math.max(count.time, link.time);
            count.version = link.version;
          }
        } else {
          loadLink(link, j, nlinks, singleAssoc);
        }
      }
      
    }

    // Maintain the counts separately
    if (bulkLoad) {
      for (LinkCount count: linkTypeCounts.values()) {
        countLoadBuffer.add(count);
        if (countLoadBuffer.size() >= bulkLoadBatchSize) {
          loadCounts(countLoadBuffer);
        }
      }
    }
    return nlinks_total;
  }

  private Link initLink() {
    Link link = new Link();
    link.link_type = LinkStore.DEFAULT_LINK_TYPE;
    link.id1_type = LinkStore.ID1_TYPE;
    link.id2_type = LinkStore.ID2_TYPE;
    link.visibility = LinkStore.VISIBILITY_DEFAULT;
    link.version = 0;
    link.data = new byte[0];
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
  private void constructLink(Random rng, Link link, long id1,
      long link_type, long outlink_ix, boolean singleAssoc) {
    link.id1 = id1;
    link.link_type = link_type;
    
    // Using random number generator for id2 means we won't know
    // which id2s exist. So link id1 to
    // maxid1 + id1 + 1 thru maxid1 + id1 + nlinks(id1) UNLESS
    // config randomid2max is nonzero.
    if (singleAssoc) {
      link.id2 = 45; // some constant
    } else {
      link.id2 = id2chooser.chooseForLoad(rng, id1, link_type,outlink_ix);
      int datasize = (int)linkDataSize.choose(rng);
      link.data = linkDataGen.fill(rng, new byte[datasize]);
    }

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("id2 chosen is " + link.id2);
    }

    // Randomize time so that id2 and timestamp aren't closely correlated
    link.time = chooseInitialTimestamp(rng);
  }


  private long chooseInitialTimestamp(Random rng) {
    // Choose something from now back to about 50 days
    return (System.currentTimeMillis() - Integer.MAX_VALUE - 1L)
                                        + rng.nextInt();
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
        stats.addStats(LinkBenchOp.LOAD_LINK, timetaken/1000, false);
  
        latencyStats.recordLatency(loaderID, 
                      LinkBenchOp.LOAD_LINK, timetaken);
      }
  
    } catch (Throwable e){//Catch exception if any
        long endtime2 = System.nanoTime();
        long timetaken2 = (endtime2 - timestart)/1000;
        logger.error("Error: " + e.getMessage(), e);
        stats.addStats(LinkBenchOp.LOAD_LINK, timetaken2, true);
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
      stats.addStats(LinkBenchOp.LOAD_LINKS_BULK, timetaken/1000, false);
      stats.addStats(LinkBenchOp.LOAD_LINKS_BULK_NLINKS, nlinks, false);
  
      latencyStats.recordLatency(loaderID, LinkBenchOp.LOAD_LINKS_BULK,
                                                             timetaken);
    } catch (Throwable e){//Catch exception if any
        long endtime2 = System.nanoTime();
        long timetaken2 = (endtime2 - timestart)/1000;
        logger.error("Error: " + e.getMessage(), e);
        stats.addStats(LinkBenchOp.LOAD_LINKS_BULK, timetaken2, true);
        store.clearErrors(loaderID);
    }
  }
  
  private void loadCounts(ArrayList<LinkCount> loadBuffer) {
    long timestart = System.nanoTime();
    
    try {
      // no inverses for now
      int ncounts = loadBuffer.size();
      store.addBulkCounts(dbid, loadBuffer);
      loadBuffer.clear();
  
      long timetaken = (System.nanoTime() - timestart);
  
      // convert to microseconds
      stats.addStats(LinkBenchOp.LOAD_COUNTS_BULK, timetaken/1000, false);
      stats.addStats(LinkBenchOp.LOAD_COUNTS_BULK_NLINKS, ncounts, false);
  
      latencyStats.recordLatency(loaderID, LinkBenchOp.LOAD_COUNTS_BULK,
                                                             timetaken);
    } catch (Throwable e){//Catch exception if any
        long endtime2 = System.nanoTime();
        long timetaken2 = (endtime2 - timestart)/1000;
        logger.error("Error: " + e.getMessage(), e);
        stats.addStats(LinkBenchOp.LOAD_COUNTS_BULK, timetaken2, true);
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
                                              0, 0, 0, 1, null);

    public LoadChunk(long id, long start, long end, Random rng) {
      this(false, id, start, end, 1, rng);
    }
    public LoadChunk(boolean shutdown,
                      long id, long start, long end, long step, Random rng) {
      super();
      this.shutdown = shutdown;
      this.id = id;
      this.start = start;
      this.end = end;
      this.step = step;
      this.size = (end - start) / step;
      this.rng = rng;
    }
    public final boolean shutdown;
    public final long id;
    public final long start;
    public final long end;
    public final long step;
    public final long size;
    public Random rng;
    
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
    /** report progress at intervals of progressReportInterval links */
    private final long progressReportInterval;
    
    public LoadProgress(Logger progressLogger,
                        long id1s_total, long progressReportInterval) {
      super();
      this.progressReportInterval = progressReportInterval;
      this.progressLogger = progressLogger;
      this.id1s_total = id1s_total;
      this.starttime_ms = 0;
      this.id1s_loaded = new AtomicLong();
      this.links_loaded = new AtomicLong();
    }
    
    public static LoadProgress create(Logger progressLogger, Properties props) {
      long maxid1 = ConfigUtil.getLong(props, Config.MAX_ID);
      long startid1 = ConfigUtil.getLong(props, Config.MIN_ID);
      long nids = maxid1 - startid1;
      long progressReportInterval = ConfigUtil.getLong(props, 
                           Config.LOAD_PROG_INTERVAL, 50000L);
      return new LoadProgress(progressLogger, nids, progressReportInterval);
    }
    
    private final Logger progressLogger;
    private final AtomicLong id1s_loaded; // progress
    private final AtomicLong links_loaded; // progress
    private final long id1s_total; // goal
    private long starttime_ms;
    
    /** Mark current time as start time for load */
    public void startTimer() {
      starttime_ms = System.currentTimeMillis();
    }
    
    /**
     * Update progress
     * @param id1_incr number of additional id1s loaded since last call
     * @param links_incr number of links loaded since last call
     */
    public void update(long id1_incr, long links_incr) {
      long curr_id1s = id1s_loaded.addAndGet(id1_incr);
      
      long curr_links = links_loaded.addAndGet(links_incr);
      long prev_links = curr_links - links_incr;
      
      if ((curr_links / progressReportInterval) >
          (prev_links / progressReportInterval) || curr_id1s == id1s_total) {
        double percentage = (curr_id1s / (double)id1s_total) * 100.0;

        // Links per second loaded
        long now = System.currentTimeMillis();
        double link_rate = ((curr_links) / ((double) now - starttime_ms))*1000;
        double id1_rate = ((curr_id1s) / ((double) now - starttime_ms))*1000;
        progressLogger.info(String.format(
            "%d/%d id1s loaded (%.1f%% complete) at %.2f id1s/sec avg. " +
            "%d links loaded at %.2f links/sec avg.", 
            curr_id1s, id1s_total, percentage, id1_rate,
            curr_links, link_rate));
      }
    }
  }
}
