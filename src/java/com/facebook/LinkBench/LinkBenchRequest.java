package com.facebook.LinkBench;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.facebook.LinkBench.RealDistribution.DistributionType;
import com.facebook.LinkBench.distributions.AccessDistributions;
import com.facebook.LinkBench.distributions.AccessDistributions.AccessDistribution;
import com.facebook.LinkBench.distributions.ID2Chooser;
import com.facebook.LinkBench.distributions.LogNormalDistribution;
import com.facebook.LinkBench.distributions.ProbabilityDistribution;
import com.facebook.LinkBench.generators.DataGenerator;
import com.facebook.LinkBench.stats.LatencyStats;
import com.facebook.LinkBench.stats.SampledStats;
import com.facebook.LinkBench.util.ClassLoadUtil;


public class LinkBenchRequest implements Runnable {
  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
  Properties props;
  LinkStore linkStore;
  NodeStore nodeStore;
  
  RequestProgress progressTracker;

  long nrequests;
  /** Requests per second: <= 0 for unlimited rate */
  private long requestrate;
  
  /** Maximum number of failed requests: < 0 for unlimited */
  private long maxFailedRequests;
  
  long maxtime;
  int nrequesters;
  int requesterID;
  long maxid1;
  long startid1;
  Level debuglevel;
  long progressfreq_ms;
  String dbid;
  boolean singleAssoc = false;

  // Control data generation settings
  private LogNormalDistribution linkDataSize;
  private DataGenerator linkAddDataGen;
  private DataGenerator linkUpDataGen;
  private LogNormalDistribution nodeDataSize;
  private DataGenerator nodeAddDataGen;
  private DataGenerator nodeUpDataGen;
  
  // cummulative percentages
  double pc_addlink;
  double pc_deletelink;
  double pc_updatelink;
  double pc_countlink;
  double pc_getlink;
  double pc_getlinklist;
  double pc_addnode;
  double pc_deletenode;
  double pc_updatenode;
  double pc_getnode;
  
  // Chance of doing historical range query
  double p_historical_getlinklist;
  
  private static class HistoryKey {
    public final long id1;
    public final long link_type;
    public HistoryKey(long id1, long link_type) {
      super();
      this.id1 = id1;
      this.link_type = link_type;
    }
    
    public HistoryKey(Link l) {
      this(l.id1, l.link_type);
    }
    
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (id1 ^ (id1 >>> 32));
      result = prime * result + (int) (link_type ^ (link_type >>> 32));
      return result;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof HistoryKey))
        return false;
      HistoryKey other = (HistoryKey) obj;
      return id1 == other.id1 && link_type == other.link_type;
    }
    
  }
  
  // Cache of last link in lists where full list wasn't retrieved
  ArrayList<Link> listTailHistory;
  
  // Index of history to avoid duplicates
  HashMap<HistoryKey, Integer> listTailHistoryIndex;
  
  // Limit of cache size
  private int listTailHistoryLimit;
  
  // Probability distribution for ids in multiget
  ProbabilityDistribution multigetDist;
  
  // Statistics
  SampledStats stats;
  LatencyStats latencyStats;

  // Other informational counters
  long numfound = 0;
  long numnotfound = 0;
  long numHistoryQueries = 0;
  
  /** 
   * Random number generator use for generating workload.  If
   * initialized with same seed, should generate same sequence of requests
   * so that tests and benchmarks are repeatable.  
   */
  Random rng;
  
  // Last node id accessed
  long lastNodeId;
  
  long requestsdone = 0;
  long errors = 0;
  boolean aborted;

  // Access distributions
  private AccessDistribution writeDist; // link writes
  private AccessDistribution writeDistUncorr; // to blend with link writes
  private double writeDistUncorrBlend; // Percentage to used writeDist2 for
  private AccessDistribution readDist; // link reads
  private AccessDistribution readDistUncorr; // to blend with link reads
  private double readDistUncorrBlend; // Percentage to used readDist2 for
  private AccessDistribution nodeReadDist; // node reads
  private AccessDistribution nodeUpdateDist; // node writes
  private AccessDistribution nodeDeleteDist; // node deletes
  
  private ID2Chooser id2chooser;
  public LinkBenchRequest(LinkStore linkStore,
                          NodeStore nodeStore,
                          Properties props,
                          LatencyStats latencyStats,
                          PrintStream csvStreamOut,
                          RequestProgress progressTracker,
                          Random rng,
                          int requesterID,
                          int nrequesters) {
    assert(linkStore != null);
    if (requesterID < 0 ||  requesterID >= nrequesters) {
      throw new IllegalArgumentException("Bad requester id " 
          + requesterID + "/" + nrequesters);
    }
    
    this.linkStore = linkStore;
    this.nodeStore = nodeStore;
    this.props = props;
    this.latencyStats = latencyStats;
    this.progressTracker = progressTracker;
    this.rng = rng;
    this.nrequesters = nrequesters;
    this.requesterID = requesterID;

    debuglevel = ConfigUtil.getDebugLevel(props);
    dbid = ConfigUtil.getPropertyRequired(props, Config.DBID);
    nrequests = ConfigUtil.getLong(props, Config.NUM_REQUESTS);
    requestrate = ConfigUtil.getLong(props, Config.REQUEST_RATE, 0L);
    maxFailedRequests = ConfigUtil.getLong(props,  Config.MAX_FAILED_REQUESTS, 0L);
    maxtime = ConfigUtil.getLong(props, Config.MAX_TIME);
    maxid1 = ConfigUtil.getLong(props, Config.MAX_ID);
    startid1 = ConfigUtil.getLong(props, Config.MIN_ID);

    // math functions may cause problems for id1 < 1
    if (startid1 <= 0) {
      throw new LinkBenchConfigError("startid1 must be >= 1");
    }
    if (maxid1 <= startid1) {
      throw new LinkBenchConfigError("maxid1 must be > startid1");
    }

    // is this a single assoc test?
    if (startid1 + 1 == maxid1) {
      singleAssoc = true;
      logger.info("Testing single row assoc read.");
    }

    initRequestProbabilities(props);
    initLinkDataGeneration(props);
    initLinkRequestDistributions(props, requesterID, nrequesters);
    if (pc_getnode > pc_getlinklist) {
      // Load stuff for node workload if needed
      if (nodeStore == null) {
        throw new IllegalArgumentException("nodeStore not provided but non-zero " +
                                         "probability of node operation");
      }
      initNodeDataGeneration(props);
      initNodeRequestDistributions(props);
    }

    long displayfreq = ConfigUtil.getLong(props, Config.DISPLAY_FREQ);
    progressfreq_ms = ConfigUtil.getLong(props, Config.PROGRESS_FREQ, 6L) * 1000;
    int maxsamples = ConfigUtil.getInt(props, Config.MAX_STAT_SAMPLES);
    stats = new SampledStats(requesterID, displayfreq, maxsamples, csvStreamOut);
   
    listTailHistoryLimit = 2048; // Hardcoded limit for now
    listTailHistory = new ArrayList<Link>(listTailHistoryLimit);
    listTailHistoryIndex = new HashMap<HistoryKey, Integer>();
    p_historical_getlinklist = ConfigUtil.getDouble(props,
                        Config.PR_GETLINKLIST_HISTORY, 0.0) / 100; 
    
    lastNodeId = startid1;
  }

  private void initRequestProbabilities(Properties props) {
    pc_addlink = ConfigUtil.getDouble(props, Config.PR_ADD_LINK);
    pc_deletelink = pc_addlink + ConfigUtil.getDouble(props, Config.PR_DELETE_LINK);
    pc_updatelink = pc_deletelink + ConfigUtil.getDouble(props, Config.PR_UPDATE_LINK);
    pc_countlink = pc_updatelink + ConfigUtil.getDouble(props, Config.PR_COUNT_LINKS);
    pc_getlink = pc_countlink + ConfigUtil.getDouble(props, Config.PR_GET_LINK);
    pc_getlinklist = pc_getlink + ConfigUtil.getDouble(props, Config.PR_GET_LINK_LIST);
    
    pc_addnode = pc_getlinklist + ConfigUtil.getDouble(props, Config.PR_ADD_NODE, 0.0);
    pc_updatenode = pc_addnode + ConfigUtil.getDouble(props, Config.PR_UPDATE_NODE, 0.0);
    pc_deletenode = pc_updatenode + ConfigUtil.getDouble(props, Config.PR_DELETE_NODE, 0.0);
    pc_getnode = pc_deletenode + ConfigUtil.getDouble(props, Config.PR_GET_NODE, 0.0);
    
    if (Math.abs(pc_getnode - 100.0) > 1e-5) {//compare real numbers
      throw new LinkBenchConfigError("Percentages of request types do not " + 
                  "add to 100, only " + pc_getnode + "!");
    }
  }

  private void initLinkRequestDistributions(Properties props, int requesterID,
      int nrequesters) {
    writeDist = AccessDistributions.loadAccessDistribution(props, 
            startid1, maxid1, DistributionType.LINK_WRITES);
    readDist = AccessDistributions.loadAccessDistribution(props, 
        startid1, maxid1, DistributionType.LINK_READS);
    
    // Load uncorrelated distributions for blending if needed
    writeDistUncorr = null;
    if (props.containsKey(Config.WRITE_UNCORR_BLEND)) {
      // Ratio of queries to use uncorrelated.  Convert from percentage
      writeDistUncorrBlend = ConfigUtil.getDouble(props, 
                Config.WRITE_UNCORR_BLEND) / 100.0;
      if (writeDistUncorrBlend > 0.0) {
        writeDistUncorr = AccessDistributions.loadAccessDistribution(props, 
            startid1, maxid1, DistributionType.LINK_WRITES_UNCORR);
      }
    }
    
    readDistUncorr = null;
    if (props.containsKey(Config.READ_UNCORR_BLEND)) {
      // Ratio of queries to use uncorrelated.  Convert from percentage
      readDistUncorrBlend = ConfigUtil.getDouble(props, 
                Config.READ_UNCORR_BLEND) / 100.0;
      if (readDistUncorrBlend > 0.0) {
        readDistUncorr = AccessDistributions.loadAccessDistribution(props, 
            startid1, maxid1, DistributionType.LINK_READS_UNCORR);
      }
    }
    
    id2chooser = new ID2Chooser(props, startid1, maxid1, 
                                nrequesters, requesterID);

    // Distribution of #id2s per multiget
    String multigetDistClass = props.getProperty(Config.LINK_MULTIGET_DIST);
    if (multigetDistClass != null && multigetDistClass.trim().length() != 0) {
      int multigetMin = ConfigUtil.getInt(props, Config.LINK_MULTIGET_DIST_MIN);
      int multigetMax = ConfigUtil.getInt(props, Config.LINK_MULTIGET_DIST_MAX);
      try {
        multigetDist = ClassLoadUtil.newInstance(multigetDistClass,
                                            ProbabilityDistribution.class);
        multigetDist.init(multigetMin, multigetMax, props, 
                                             Config.LINK_MULTIGET_DIST_PREFIX);
      } catch (ClassNotFoundException e) {
        logger.error(e);
        throw new LinkBenchConfigError("Class" + multigetDistClass + 
            " could not be loaded as ProbabilityDistribution");
      }
    } else {
      multigetDist = null;
    }
  }

  private void initLinkDataGeneration(Properties props) {
    try {
      double medLinkDataSize = ConfigUtil.getDouble(props, 
                                            Config.LINK_DATASIZE);
      linkDataSize = new LogNormalDistribution();
      linkDataSize.init(0, LinkStore.MAX_LINK_DATA, medLinkDataSize,
                           Config.LINK_DATASIZE_SIGMA);
      linkAddDataGen = ClassLoadUtil.newInstance(
          ConfigUtil.getPropertyRequired(props, Config.LINK_ADD_DATAGEN),
          DataGenerator.class);
      linkAddDataGen.init(props, Config.LINK_ADD_DATAGEN_PREFIX);
      
      linkUpDataGen = ClassLoadUtil.newInstance(
          ConfigUtil.getPropertyRequired(props, Config.LINK_UP_DATAGEN),
          DataGenerator.class);
      linkUpDataGen.init(props, Config.LINK_UP_DATAGEN_PREFIX);
    } catch (ClassNotFoundException ex) {
      logger.error(ex);
      throw new LinkBenchConfigError("Error loading data generator class: " 
            + ex.getMessage());
    }
  }

  private void initNodeRequestDistributions(Properties props) {
    try {
      nodeReadDist  = AccessDistributions.loadAccessDistribution(props, 
        startid1, maxid1, DistributionType.NODE_READS);
    } catch (LinkBenchConfigError e) {
      // Not defined
      logger.info("Node access distribution not configured: " +
          e.getMessage());
      throw new LinkBenchConfigError("Node read distribution not " +
            "configured but node read operations have non-zero probability");
    }
    
    try {
      nodeUpdateDist  = AccessDistributions.loadAccessDistribution(props, 
        startid1, maxid1, DistributionType.NODE_UPDATES);
    } catch (LinkBenchConfigError e) {
      // Not defined
      logger.info("Node access distribution not configured: " +
              e.getMessage());
      throw new LinkBenchConfigError("Node write distribution not " +
            "configured but node write operations have non-zero probability");
    }
    
    try {
      nodeDeleteDist = AccessDistributions.loadAccessDistribution(props, 
        startid1, maxid1, DistributionType.NODE_DELETES);
    } catch (LinkBenchConfigError e) {
      // Not defined
      logger.info("Node delete distribution not configured: " +
              e.getMessage());
      throw new LinkBenchConfigError("Node delete distribution not " +
            "configured but node write operations have non-zero probability");
    }
  }

  private void initNodeDataGeneration(Properties props) {
    try {  
      double medNodeDataSize = ConfigUtil.getDouble(props, 
                                              Config.NODE_DATASIZE);
      nodeDataSize = new LogNormalDistribution();
      nodeDataSize.init(0, NodeStore.MAX_NODE_DATA, medNodeDataSize,
                        Config.NODE_DATASIZE_SIGMA);

      String dataGenClass = ConfigUtil.getPropertyRequired(props, 
                                         Config.NODE_ADD_DATAGEN);
      nodeAddDataGen = ClassLoadUtil.newInstance(dataGenClass,
                                                 DataGenerator.class);
      nodeAddDataGen.init(props, Config.NODE_ADD_DATAGEN_PREFIX);
      
      dataGenClass = ConfigUtil.getPropertyRequired(props, 
                        Config.NODE_UP_DATAGEN);
      nodeUpDataGen = ClassLoadUtil.newInstance(dataGenClass,
                                                 DataGenerator.class);
      nodeUpDataGen.init(props, Config.NODE_UP_DATAGEN_PREFIX);
    } catch (ClassNotFoundException ex) {
      logger.error(ex);
      throw new LinkBenchConfigError("Error loading data generator class: " 
            + ex.getMessage());
    }
  }

  public long getRequestsDone() {
    return requestsdone;
  }
  
  public boolean didAbort() {
    return aborted;
  }

  // gets id1 for the request based on desired distribution
  private long chooseRequestID(DistributionType type, long previousId1) {
    AccessDistribution dist;
    switch (type) {
    case LINK_READS:
      // Blend between distributions if needed
      if (readDistUncorr == null || rng.nextDouble() >= readDistUncorrBlend) {
        dist = readDist;
      } else {
        dist = readDistUncorr;
      }
      break;
    case LINK_WRITES:
      // Blend between distributions if needed
      if (writeDistUncorr == null || rng.nextDouble() >= writeDistUncorrBlend) {
        dist = writeDist;
      } else {
        dist = writeDistUncorr;
      }
      break;
    case LINK_WRITES_UNCORR:
      dist = writeDistUncorr;
      break;
    case NODE_READS:
      dist = nodeReadDist;
      break;
    case NODE_UPDATES:
      dist = nodeUpdateDist;
      break;
    case NODE_DELETES:
      dist = nodeDeleteDist;
      break;
    default:
      throw new RuntimeException("Unknown value for type: " + type);
    }
    long newid1 = dist.nextID(rng, previousId1);
    // Distribution responsible for generating number in range
    assert((newid1 >= startid1) && (newid1 < maxid1));
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("id1 generated = " + newid1 +
         " for access distribution: " + dist.getClass().getName() + ": " +
         dist.toString());
    }
    
    if (dist.getShuffler() != null) {
      // Shuffle to go from position in space ranked from most to least accessed,
      // to the real id space
      newid1 = startid1 + dist.getShuffler().permute(newid1 - startid1);
    }
    return newid1;
  }

  /**
   * Randomly choose a single request and execute it, updating
   * statistics
   * @param requestno
   * @return true if successful, false on error
   */
  private boolean onerequest(long requestno) {

    double r = rng.nextDouble() * 100.0;

    long starttime = 0;
    long endtime = 0;

    LinkBenchOp type = LinkBenchOp.UNKNOWN; // initialize to invalid value
    Link link = new Link();
    try {

      if (r <= pc_addlink) {
        // generate add request
        type = LinkBenchOp.ADD_LINK;
        link.id1 = chooseRequestID(DistributionType.LINK_WRITES, link.id1);
        link.link_type = id2chooser.chooseRandomLinkType(rng);
        link.id2 = id2chooser.chooseForOp(rng, link.id1, link.link_type,
                                                ID2Chooser.P_ADD_EXIST);
        link.id1_type = LinkStore.ID1_TYPE;
        link.id2_type = LinkStore.ID2_TYPE;
        link.visibility = LinkStore.VISIBILITY_DEFAULT;
        link.version = 0;
        link.time = System.currentTimeMillis();
        link.data = linkAddDataGen.fill(rng, 
                                      new byte[(int)linkDataSize.choose(rng)]);

        starttime = System.nanoTime();
        // no inverses for now
        boolean alreadyExists = linkStore.addLink(dbid, link, true);
        boolean added = !alreadyExists;
        endtime = System.nanoTime();
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("addLink id1=" + link.id1 + " link_type=" 
                    + link.link_type + " id2=" + link.id2 + " added=" + added);
        } 
      } else if (r <= pc_deletelink) {
        type = LinkBenchOp.DELETE_LINK;
        long id1 = chooseRequestID(DistributionType.LINK_WRITES, link.id1);
        long link_type = id2chooser.chooseRandomLinkType(rng);
        long id2 = id2chooser.chooseForOp(rng, id1, link_type,
                                          ID2Chooser.P_DELETE_EXIST);
        starttime = System.nanoTime();
        linkStore.deleteLink(dbid, id1, link_type, id2,
         true, // no inverse
         false);
        endtime = System.nanoTime();
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("deleteLink id1=" + id1 + " link_type=" + link_type 
                     + " id2=" + id2);
        } 
      } else if (r <= pc_updatelink) {
        type = LinkBenchOp.UPDATE_LINK;
        link.id1 = chooseRequestID(DistributionType.LINK_WRITES, link.id1);
        link.link_type = id2chooser.chooseRandomLinkType(rng);
        // Update one of the existing links
        link.id2 = id2chooser.chooseForOp(rng, link.id1, link.link_type,
                                              ID2Chooser.P_UPDATE_EXIST);
        link.id1_type = LinkStore.ID1_TYPE;
        link.id2_type = LinkStore.ID2_TYPE;
        link.visibility = LinkStore.VISIBILITY_DEFAULT;
        link.version = 0;
        link.time = System.currentTimeMillis();
        link.data = linkUpDataGen.fill(rng, 
                            new byte[(int)linkDataSize.choose(rng)]);   

        starttime = System.nanoTime();
        // no inverses for now
        boolean found1 = linkStore.addLink(dbid, link, true);
        boolean found = found1;
        endtime = System.nanoTime();
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("updateLink id1=" + link.id1 + " link_type=" 
                + link.link_type + " id2=" + link.id2 + " found=" + found);
        } 
      } else if (r <= pc_countlink) {

        type = LinkBenchOp.COUNT_LINK;

        long id1 = chooseRequestID(DistributionType.LINK_READS, link.id1);
        long link_type = id2chooser.chooseRandomLinkType(rng);
        starttime = System.nanoTime();
        long count = linkStore.countLinks(dbid, id1, link_type);
        endtime = System.nanoTime();
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("countLink id1=" + id1 + " link_type=" + link_type 
                     + " count=" + count);
        } 
      } else if (r <= pc_getlink) {

        type = LinkBenchOp.MULTIGET_LINK;

        long id1 = chooseRequestID(DistributionType.LINK_READS, link.id1);
        long link_type = id2chooser.chooseRandomLinkType(rng);
        int nid2s = 1;
        if (multigetDist != null) { 
          nid2s = (int)multigetDist.choose(rng);
        }
        long id2s[] = id2chooser.chooseMultipleForOp(rng, id1, link_type, nid2s,
                                                 ID2Chooser.P_GET_EXIST);

        starttime = System.nanoTime();
        int found = getLink(id1, link_type, id2s);
        assert(found >= 0 && found <= nid2s);
        endtime = System.nanoTime();

        if (found > 0) {
          numfound += found;
        } else {
          numnotfound += nid2s - found;
        }

      } else if (r <= pc_getlinklist) {

        type = LinkBenchOp.GET_LINKS_LIST;
        Link links[];
        
        if (rng.nextDouble() < p_historical_getlinklist &&
                    !this.listTailHistory.isEmpty()) {
          links = getLinkListTail();
        } else {
          long id1 = chooseRequestID(DistributionType.LINK_READS, link.id1);
          long link_type = id2chooser.chooseRandomLinkType(rng);
          starttime = System.nanoTime();
          links = getLinkList(id1, link_type);
          endtime = System.nanoTime();
        }
        
        int count = ((links == null) ? 0 : links.length);
        stats.addStats(LinkBenchOp.RANGE_SIZE, count, false);
      } else if (r <= pc_addnode) {
        type = LinkBenchOp.ADD_NODE;
        Node newNode = createAddNode();
        starttime = System.nanoTime();
        lastNodeId = nodeStore.addNode(dbid, newNode);
        endtime = System.nanoTime();
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("addNode " + newNode);
        }
      } else if (r <= pc_updatenode) {
        type = LinkBenchOp.UPDATE_NODE;
        // Choose an id that has previously been created (but might have
        // been since deleted
        long upId = chooseRequestID(DistributionType.NODE_UPDATES, 
                                     lastNodeId);
        // Generate new data randomly
        Node newNode = createUpdateNode(upId);
        
        starttime = System.nanoTime();
        boolean changed = nodeStore.updateNode(dbid, newNode);
        endtime = System.nanoTime();
        lastNodeId = upId;
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("updateNode " + newNode + " changed=" + changed);
        } 
      } else if (r <= pc_deletenode) {
        type = LinkBenchOp.DELETE_NODE;
        long idToDelete = chooseRequestID(DistributionType.NODE_DELETES, 
                                          lastNodeId);
        starttime = System.nanoTime();
        boolean deleted = nodeStore.deleteNode(dbid, LinkStore.ID1_TYPE,
                                                     idToDelete);
        endtime = System.nanoTime();
        lastNodeId = idToDelete;
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("deleteNode " + idToDelete + " deleted=" + deleted);
        }
      } else if (r <= pc_getnode) {
        type = LinkBenchOp.GET_NODE;
        starttime = System.nanoTime();
        long idToFetch = chooseRequestID(DistributionType.NODE_READS, 
                                         lastNodeId);
        Node fetched = nodeStore.getNode(dbid, LinkStore.ID1_TYPE, idToFetch);
        endtime = System.nanoTime();
        lastNodeId = idToFetch;
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          if (fetched == null) {
            logger.trace("getNode " + idToFetch + " not found");
          } else {
            logger.trace("getNode " + fetched);
          }
        }
      } else {
        logger.error("No-op in requester: last probability < 1.0");
        return false;
      }


      // convert to microseconds
      long timetaken = (endtime - starttime)/1000;

      // record statistics
      stats.addStats(type, timetaken, false);
      latencyStats.recordLatency(requesterID, type, timetaken);

      return true;
    } catch (Throwable e){//Catch exception if any

      long endtime2 = System.nanoTime();

      long timetaken2 = (endtime2 - starttime)/1000;

      logger.error(type.displayName() + " error " +
                         e.getMessage(), e);

      stats.addStats(type, timetaken2, true);
      linkStore.clearErrors(requesterID);
      return false;
    }


  }

  /**
   * Create a new node for adding to database
   * @return
   */
  private Node createAddNode() {
    byte data[] = nodeAddDataGen.fill(rng, new byte[(int)nodeDataSize.choose(rng)]);
    return new Node(-1, LinkStore.ID1_TYPE, 1, 
                    (int)(System.currentTimeMillis()/1000), data);
  }
  
  /**
   * Create new node for updating in database
   */
  private Node createUpdateNode(long id) {
    byte data[] = nodeUpDataGen.fill(rng, new byte[(int)nodeDataSize.choose(rng)]);
    return new Node(id, LinkStore.ID1_TYPE, 2, 
                    (int)(System.currentTimeMillis()/1000), data);
  }

  @Override
  public void run() {
    logger.info("Requester thread #" + requesterID + " started: will do "
        + nrequests + " ops.");
    logger.debug("Requester thread #" + requesterID + " first random number "
                  + rng.nextLong());
    
    try {
      this.linkStore.initialize(props, Phase.REQUEST, requesterID);
      if (this.nodeStore != null && this.nodeStore != this.linkStore) {
        this.nodeStore.initialize(props, Phase.REQUEST, requesterID);
      }
    } catch (Exception e) {
      logger.error("Error while initializing store", e);
      throw new RuntimeException(e);
    }
    
    long starttime = System.currentTimeMillis();
    long endtime = starttime + maxtime * 1000;
    long lastupdate = starttime;
    long curtime = 0;
    long i;

    if (singleAssoc) {
      LinkBenchOp type = LinkBenchOp.UNKNOWN;
      try {
        Link link = new Link();
        // add a single assoc to the database
        link.id1 = 45;
        link.id1 = 46;
        type = LinkBenchOp.ADD_LINK;
        // no inverses for now
        boolean alreadyExists = linkStore.addLink(dbid, link, true);
        boolean addLink = !alreadyExists;

        // read this assoc from the database over and over again
        type = LinkBenchOp.MULTIGET_LINK;
        for (i = 0; i < nrequests; i++) {
          int found = getLink(link.id1, link.link_type,
                                  new long[]{link.id2});
          if (found == 1) {
            requestsdone++;
          } else {
            logger.warn("ThreadID = " + requesterID +
                               " not found link for id1=45");
          }
        }
      } catch (Throwable e) {
        logger.error(type.displayName() + "error " +
                         e.getMessage(), e);
        aborted = true;
      }
      return;
    }
    
    int requestsSinceLastUpdate = 0;
    long reqTime_ns = System.nanoTime();
    double requestrate_ns = ((double)requestrate)/1e9;
    for (i = 0; i < nrequests; i++) {
      if (requestrate > 0) {
        reqTime_ns = Timer.waitExpInterval(rng, reqTime_ns, requestrate_ns);
      }
      boolean success = onerequest(i);
      requestsdone++;
      if (!success) {
        errors++;
        if (maxFailedRequests >= 0 && errors > maxFailedRequests) {
          logger.error(String.format("Requester #%d aborting: %d failed requests" +
          		" (out of %d total) ", requesterID, errors, requestsdone));
          aborted = true;
          return;
        }
      }
      
      curtime = System.currentTimeMillis();
      
      if (curtime > lastupdate + progressfreq_ms) {
        logger.info(String.format("Requester #%d %d/%d requests done",
            requesterID, requestsdone, nrequests));
        lastupdate = curtime;
      }
      
      requestsSinceLastUpdate++;
      if (curtime > endtime) {
        break;
      }
      if (requestsSinceLastUpdate >= RequestProgress.THREAD_REPORT_INTERVAL) {
        progressTracker.update(requestsSinceLastUpdate);
        requestsSinceLastUpdate = 0;
      }
    }
    
    progressTracker.update(requestsSinceLastUpdate);

    stats.displayStats(System.currentTimeMillis(), Arrays.asList(
        LinkBenchOp.MULTIGET_LINK, LinkBenchOp.GET_LINKS_LIST,
        LinkBenchOp.COUNT_LINK,
        LinkBenchOp.UPDATE_LINK, LinkBenchOp.ADD_LINK, 
        LinkBenchOp.RANGE_SIZE, LinkBenchOp.ADD_NODE,
        LinkBenchOp.UPDATE_NODE, LinkBenchOp.DELETE_NODE,
        LinkBenchOp.GET_NODE));
    logger.info("ThreadID = " + requesterID +
                       " total requests = " + i +
                       " requests/second = " + ((1000 * i)/(curtime - starttime)) +
                       " found = " + numfound +
                       " not found = " + numnotfound +
                       " history queries = " + numHistoryQueries + "/" +
                                   stats.getCount(LinkBenchOp.GET_LINKS_LIST));

  }

  int getLink(long id1, long link_type, long id2s[]) throws Exception {
    Link links[] = linkStore.multigetLinks(dbid, id1, link_type, id2s);
    return links == null ? 0 : links.length;
  }

  Link[] getLinkList(long id1, long link_type) throws Exception {
    Link links[] = linkStore.getLinkList(dbid, id1, link_type);
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
       logger.trace("getLinkList(id1=" + id1 + ", link_type="  + link_type
                     + ") => count=" + (links == null ? 0 : links.length));
    }
    // If there were more links than limit, record
    if (links != null && links.length >= linkStore.getRangeLimit()) {
      Link lastLink = links[links.length-1];
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("Maybe more history for (" + id1 +"," + 
                      link_type + " older than " + lastLink.time);
      }
      
      addTailCacheEntry(lastLink);
    }
    return links;
  }

  Link[] getLinkListTail() throws Exception {
    assert(!listTailHistoryIndex.isEmpty());
    assert(!listTailHistory.isEmpty());
    int choice = rng.nextInt(listTailHistory.size());
    Link prevLast = listTailHistory.get(choice);
    
    // Get links past the oldest last retrieved
    Link links[] = linkStore.getLinkList(dbid, prevLast.id1,
        prevLast.link_type, 0, prevLast.time, 1, linkStore.getRangeLimit());
    
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("getLinkListTail(id1=" + prevLast.id1 + ", link_type=" 
                + prevLast.link_type + ", max_time=" + prevLast.time
                + " => count=" + (links == null ? 0 : links.length));
   }
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("Historical range query for (" + prevLast.id1 +"," +
                    prevLast.link_type + " older than " + prevLast.time +
                    ": " + (links == null ? 0 : links.length) + " results");
    }
    
    if (links != null && links.length == linkStore.getRangeLimit()) {
      // There might be yet more history
      Link last = links[links.length-1];
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("might be yet more history for (" + last.id1 +"," +
                      last.link_type + " older than " + last.time);
      }
      // Update in place
      listTailHistory.set(choice, last.clone());
    } else {
      // No more history after this, remove from cache
      removeTailCacheEntry(choice, null); 
    }
    numHistoryQueries++;
    return links;
  }

  /**
   * Add a new link to the history cache, unless already present
   * @param lastLink the last (i.e. lowest timestamp) link retrieved 
   */
  private void addTailCacheEntry(Link lastLink) {
    HistoryKey key = new HistoryKey(lastLink);
    if (listTailHistoryIndex.containsKey(key)) {
      // Already present
      return;
    }
    
    if (listTailHistory.size() < listTailHistoryLimit) {
      listTailHistory.add(lastLink.clone());
      listTailHistoryIndex.put(key, listTailHistory.size() - 1);
    } else {
      // Need to evict entry
      int choice = rng.nextInt(listTailHistory.size());
      removeTailCacheEntry(choice, lastLink.clone());
    }
  }

  /**
   * Remove or replace entry in listTailHistory and update index
   * @param pos index of entry in listTailHistory
   * @param repl replace with this if not null
   */
  private void removeTailCacheEntry(int pos, Link repl) {
    Link entry = listTailHistory.get(pos);
    if (pos == listTailHistory.size() - 1) {
      // removing from last position, don't need to fill gap
      listTailHistoryIndex.remove(new HistoryKey(entry));
      int lastIx = listTailHistory.size() - 1;
      if (repl == null) {
        listTailHistory.remove(lastIx);
      } else {
        listTailHistory.set(lastIx, repl);
        listTailHistoryIndex.put(new HistoryKey(repl), lastIx);
      }
    } else { 
      if (repl == null) {
        // Replace with last entry in cache to fill gap
        repl = listTailHistory.get(listTailHistory.size() - 1);
        listTailHistory.remove(listTailHistory.size() - 1);
      }
      listTailHistory.set(pos, repl);
      listTailHistoryIndex.put(new HistoryKey(repl), pos);
    }
  }

  public static class RequestProgress {
    // How many ops before a thread should register its progress
    static final int THREAD_REPORT_INTERVAL = 250;
    /** How many ops before a progress update should be printed to console */
    private final long interval;
    
    private final Logger progressLogger;
    
    private long totalRequests;
    private final AtomicLong requestsDone;
    
    private long startTime;
    private long timeLimit_s;

    public RequestProgress(Logger progressLogger, long totalRequests,
                            long timeLimit, long interval) {
      this.interval = interval;
      this.progressLogger = progressLogger;
      this.totalRequests = totalRequests;
      this.requestsDone = new AtomicLong();
      this.timeLimit_s = timeLimit;
      this.startTime = 0;
    }
    
    public void startTimer() {
      startTime = System.currentTimeMillis();
    }
    
    public void update(long requestIncr) {
      long curr = requestsDone.addAndGet(requestIncr);
      long prev = curr - requestIncr;
      
      if ((curr / interval) > (prev / interval) || curr == totalRequests) {
        float progressPercent = ((float) curr) / totalRequests * 100;
        long now = System.currentTimeMillis();
        long elapsed = now - startTime;
        float elapsed_s = ((float) elapsed) / 1000;
        float limitPercent = (elapsed_s / ((float) timeLimit_s)) * 100;
        float rate = curr / ((float)elapsed_s);
        progressLogger.info(String.format(
            "%d/%d requests finished: %.1f%% complete at %.1f ops/sec" +
            " %.1f/%d secs elapsed: %.1f%% of time limit used",
            curr, totalRequests, progressPercent, rate,
            elapsed_s, timeLimit_s, limitPercent));
            
      }
    }
  }

  public static RequestProgress createProgress(Logger logger,
       Properties props) {
    long total_requests = ConfigUtil.getLong(props, Config.NUM_REQUESTS)
                      * ConfigUtil.getLong(props, Config.NUM_REQUESTERS);
    long progressInterval = ConfigUtil.getLong(props, Config.REQ_PROG_INTERVAL,
                                               10000L);
    return new RequestProgress(logger, total_requests,
        ConfigUtil.getLong(props, Config.MAX_TIME), progressInterval);
  }
}

