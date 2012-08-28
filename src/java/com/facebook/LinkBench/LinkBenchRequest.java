package com.facebook.LinkBench;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.facebook.LinkBench.distributions.ProbabilityDistribution;
import com.facebook.LinkBench.distributions.UniformDistribution;
import com.facebook.LinkBench.generators.UniformDataGenerator;
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
  long randomid2max;
  long maxid1;
  long startid1;
  int datasize;
  Level debuglevel;
  long progressfreq_ms;
  String dbid;
  boolean singleAssoc = false;

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
  
  
  LinkBenchStats stats;
  LinkBenchLatency latencyStats;

  long numfound;
  long numnotfound;

  /** 
   * Random number generator use for generating workload.  If
   * initialized with same seed, should generate same sequence of requests
   * so that tests and benchmarks are repeatable.  
   */
  Random rng;

  Link link;

  // #links distribution from properties file
  int nlinks_func;
  int nlinks_config;
  int nlinks_default;

  long requestsdone = 0;
  long errors = 0;
  boolean aborted;
  
  // configuration for generating id2
  int id2gen_config;

  private int wr_distrfunc;

  private int wr_distrconfig;

  private int rd_distrfunc;

  private int rd_distrconfig;
  
  // Access distribution for nodes
  private ProbabilityDistribution nodeAccessDist;
  // TODO: temporarily hardcode some shuffling paramters
  private static final long[] NODE_ACCESS_SHUFFLER_PARAMS = {27, 13};
  
  public LinkBenchRequest(LinkStore linkStore,
                          NodeStore nodeStore,
                          Properties props,
                          LinkBenchLatency latencyStats,
                          RequestProgress progressTracker,
                          Random rng,
                          int requesterID,
                          int nrequesters) {
    assert(linkStore != null);
    
    this.linkStore = linkStore;
    this.nodeStore = nodeStore;
    this.props = props;
    this.latencyStats = latencyStats;
    this.progressTracker = progressTracker;
    this.rng = rng;

    this.nrequesters = nrequesters;
    this.requesterID = requesterID;
    if (requesterID < 0 ||  requesterID >= nrequesters) {
      throw new IllegalArgumentException("Bad requester id " 
          + requesterID + "/" + nrequesters);
    }
    
    nrequests = Long.parseLong(props.getProperty(Config.NUM_REQUESTS));
    requestrate = Long.parseLong(props.getProperty(Config.REQUEST_RATE, "0"));
   
    maxFailedRequests = Long.parseLong(props.getProperty(
                                                Config.MAX_FAILED_REQUESTS, "0"));
    
    maxtime = Long.parseLong(props.getProperty(Config.MAX_TIME));
    maxid1 = Long.parseLong(props.getProperty(Config.MAX_ID));
    startid1 = Long.parseLong(props.getProperty(Config.MIN_ID));

    // math functions may cause problems for id1 = 0. Start at 1.
    if (startid1 <= 0) {
      startid1 = 1;
    }

    // is this a single assoc test?
    if (startid1 + 1 == maxid1) {
      singleAssoc = true;
      logger.info("Testing single row assoc read.");
    }

    datasize = Integer.parseInt(props.getProperty(Config.LINK_DATASIZE));
    debuglevel = ConfigUtil.getDebugLevel(props);
    dbid = props.getProperty(Config.DBID);

    pc_addlink = Double.parseDouble(props.getProperty(Config.PR_ADD_LINK));
    pc_deletelink = pc_addlink + Double.parseDouble(props.getProperty(Config.PR_DELETE_LINK));
    pc_updatelink = pc_deletelink + Double.parseDouble(props.getProperty(Config.PR_UPDATE_LINK));
    pc_countlink = pc_updatelink + Double.parseDouble(props.getProperty(Config.PR_COUNT_LINKS));
    pc_getlink = pc_countlink + Double.parseDouble(props.getProperty(Config.PR_GET_LINK));
    pc_getlinklist = pc_getlink + Double.parseDouble(props.getProperty(Config.PR_GET_LINK_LIST));
    
    pc_addnode = pc_getlinklist + Double.parseDouble(props.getProperty(Config.PR_ADD_NODE, "0.0"));
    pc_updatenode = pc_addnode + Double.parseDouble(props.getProperty(Config.PR_UPDATE_NODE, "0.0"));
    pc_deletenode = pc_updatenode + Double.parseDouble(props.getProperty(Config.PR_DELETE_NODE, "0.0"));
    pc_getnode = pc_deletenode + Double.parseDouble(props.getProperty(Config.PR_GET_NODE, "0.0"));
    if (pc_getnode > pc_getlinklist && nodeStore == null) {
      throw new IllegalArgumentException("nodeStore not provided but non-zero " +
      		                               "probability of node operation");
    }
    
    if (Math.abs(pc_getnode - 100.0) > 1e-5) {//compare real numbers
      logger.error("Percentages of request types do not add to 100!");
      System.exit(1);
    }
    
    wr_distrfunc = Integer.parseInt(props.getProperty(Config.WRITE_FUNCTION));
    wr_distrconfig = Integer.parseInt(props.getProperty(Config.WRITE_CONFIG));
    rd_distrfunc = Integer.parseInt(props.getProperty(Config.READ_FUNCTION));
    rd_distrconfig = Integer.parseInt(props.getProperty(Config.READ_CONFIG));

    String nodeAccessDistClass = props.getProperty(Config.NODE_ACCESS_DIST,
                                       UniformDistribution.class.getName());
    try {
      nodeAccessDist = ClassLoadUtil.newInstance(nodeAccessDistClass,
                                              ProbabilityDistribution.class);
    } catch (ClassNotFoundException ex) {
      logger.error("Class load error:", ex);
      throw new RuntimeException(ex);
    }
    
    nodeAccessDist.init(startid1, maxid1, props, 
                                    Config.NODE_ACCESS_DIST_KEY_PREFIX);

    numfound = 0;
    numnotfound = 0;

    long displayfreq = Long.parseLong(props.getProperty(Config.DISPLAY_FREQ));
    String progressfreq = props.getProperty(Config.PROGRESS_FREQ);
    if (progressfreq == null) {
      progressfreq_ms = 6000L;
    } else {
      progressfreq_ms = Long.parseLong(progressfreq) * 1000L;
    }
    int maxsamples = Integer.parseInt(props.getProperty(Config.MAX_STAT_SAMPLES));
    stats = new LinkBenchStats(requesterID, displayfreq, maxsamples);
    
    // random number generator for id2
    randomid2max = Long.parseLong(props.getProperty(Config.RANDOM_ID2_MAX));

    // configuration for generating id2
    id2gen_config = Integer.parseInt(props.getProperty(Config.ID2GEN_CONFIG));

    link = new Link();

    nlinks_func = Integer.parseInt(props.getProperty(Config.NLINKS_FUNC));
    nlinks_config = Integer.parseInt(props.getProperty(Config.NLINKS_CONFIG));
    nlinks_default = Integer.parseInt(props.getProperty(Config.NLINKS_DEFAULT));
    if (nlinks_func == -2) {//real distribution has its own initialization
      try {
        //in case there is no load phase, real distribution
        //will be initialized here
        RealDistribution.loadOneShot(props);
      } catch (Exception e) {
        logger.error(e);
        System.exit(1);
      }
    }
  }

  public long getRequestsDone() {
    return requestsdone;
  }
  
  public boolean didAbort() {
    return aborted;
  }

  // gets id1 for the request based on desired distribution
  private long chooseRequestID1(boolean write,
                                        long previousId1) {

    // Need to pick a random number between startid1 (inclusive) and maxid1
    // (exclusive)
    long longid1 = startid1 +
      Math.abs(rng.nextLong())%(maxid1 - startid1);
    double id1 = longid1;
    double drange = (double)maxid1 - startid1;

    int distrfunc = write ? wr_distrfunc : rd_distrfunc;
    int distrconfig = write ? wr_distrconfig : rd_distrconfig;
    
    long newid1;
    switch(distrfunc) {

    case -3: //sequential from startid1 to maxid1 (circular)
      if (previousId1 <= 0) {
        newid1 = startid1;
      } else {
        newid1 = previousId1+1;
        if (newid1 > maxid1) {
          newid1 = startid1;
        }
      }
      break;

    case -2: //real distribution
      newid1 = RealDistribution.getNextId1(rng, startid1, maxid1, write);
      break;

    case -1 : // inverse function f(x) = 1/x.
      newid1 = (long)(Math.ceil(drange/id1));
      break;
    case 0 : // generate id1 that is even multiple of distrconfig
      newid1 = distrconfig * (long)(Math.ceil(id1/distrconfig));
      break;
    case 100 : // generate id1 that is power of distrconfig
      double log = Math.ceil(Math.log(id1)/Math.log(distrconfig));

      newid1 = (long)Math.pow(distrconfig, log);
      break;
    default: // generate id1 that is perfect square if distrfunc is 2,
      // perfect cube if distrfunc is 3 etc

      // get the nth root where n = distrconfig
      long nthroot = (long)Math.ceil(Math.pow(id1, (1.0)/distrfunc));

      // get nthroot raised to power n
      newid1 = (long)Math.pow(nthroot, distrfunc);
      break;
    }

    if ((newid1 >= startid1) && (newid1 < maxid1)) {
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("id1 generated = " + newid1 +
                             " for (distrfunc, distrconfig): " +
                             distrfunc + "," +  distrconfig);
      }

      return newid1;
    } else if (newid1 < startid1) {
      longid1 = startid1;
    } else if (newid1 >= maxid1) {
      longid1 = maxid1 - 1;
    }

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.debug("Using " + longid1 + " as id1 generated = " + newid1 +
                         " out of bounds for (distrfunc, distrconfig): " +
                         distrfunc + "," +  distrconfig);
    }

    return longid1;
  }

  /** 
   * Choose an ID in the range of currently existing IDs
   * based on the access distribution for nodes
   * @return
   */
  private long chooseRequestNodeID() {
    // Shuffle by different values to the link accesses
    long unshuffled = nodeAccessDist.choose(rng);
    long shuffled = Shuffler.getPermutationValue(unshuffled, startid1, maxid1,
                        NODE_ACCESS_SHUFFLER_PARAMS);
    return shuffled;
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

    try {

      if (r <= pc_addlink) {
        // generate add request
        type = LinkBenchOp.ADD_LINK;
        link.id1 = chooseRequestID1(true, link.id1);
        starttime = System.nanoTime();
        addLink(link);
        endtime = System.nanoTime();
      } else if (r <= pc_deletelink) {
        type = LinkBenchOp.DELETE_LINK;
        link.id1 = chooseRequestID1(true, link.id1);
        starttime = System.nanoTime();
        deleteLink(link);
        endtime = System.nanoTime();
      } else if (r <= pc_updatelink) {
        type = LinkBenchOp.UPDATE_LINK;
        link.id1 = chooseRequestID1(true, link.id1);
        starttime = System.nanoTime();
        updateLink(link);
        endtime = System.nanoTime();
      } else if (r <= pc_countlink) {

        type = LinkBenchOp.COUNT_LINK;

        link.id1 = chooseRequestID1(false, link.id1);
        starttime = System.nanoTime();
        countLinks(link);
        endtime = System.nanoTime();

      } else if (r <= pc_getlink) {

        type = LinkBenchOp.GET_LINK;


        link.id1 = chooseRequestID1(false, link.id1);


        long nlinks = LinkBenchLoad.getNlinks(rng,
            link.id1, startid1, maxid1,
            nlinks_func, nlinks_config, nlinks_default);

        // id1 is expected to have nlinks links. Retrieve one of those.
        link.id2 = (randomid2max == 0 ?
                     (maxid1 + link.id1 + rng.nextInt((int)nlinks + 1)) :
                     rng.nextInt((int)randomid2max));

        starttime = System.nanoTime();
        boolean found = getLink(link);
        endtime = System.nanoTime();

        if (found) {
          numfound++;
        } else {
          numnotfound++;
        }

      } else if (r <= pc_getlinklist) {

        type = LinkBenchOp.GET_LINKS_LIST;

        link.id1 = chooseRequestID1(false, link.id1);
        starttime = System.nanoTime();
        Link links[] = getLinkList(link);
        endtime = System.nanoTime();
        int count = ((links == null) ? 0 : links.length);

        stats.addStats(LinkBenchOp.RANGE_SIZE, count, false);
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("getlinklist count = " + count);
        }
      } else if (r <= pc_addnode) {
        type = LinkBenchOp.ADD_NODE;
        Node newNode = createNode();
        starttime = System.nanoTime();
        long newId = nodeStore.addNode(dbid, newNode);
        endtime = System.nanoTime();
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("addNode " + newNode);
        }
      } else if (r <= pc_updatenode) {
        type = LinkBenchOp.UPDATE_NODE;
        // Generate new data randomly
        Node newNode = createNode();
        // Choose an id that has previously been created (but might have
        // been since deleted
        newNode.id = chooseRequestNodeID();
        starttime = System.nanoTime();
        boolean changed = nodeStore.updateNode(dbid, newNode);
        endtime = System.nanoTime();
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("updateNode " + newNode + " changed=" + changed);
        }
      } else if (r <= pc_deletenode) {
        type = LinkBenchOp.DELETE_NODE;
        long idToDelete = chooseRequestNodeID();
        starttime = System.nanoTime();
        boolean deleted = nodeStore.deleteNode(dbid, LinkStore.ID1_TYPE,
                                                     idToDelete);
        endtime = System.nanoTime();
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("deleteNode " + idToDelete + " deleted=" + deleted);
        }
      } else if (r <= pc_getnode) {
        type = LinkBenchOp.GET_NODE;
        starttime = System.nanoTime();
        long idToFetch = chooseRequestNodeID();
        Node fetched = nodeStore.getNode(dbid, LinkStore.ID1_TYPE, idToFetch);
        endtime = System.nanoTime();
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

  private Node createNode() {
    // TODO put in some real data
    return new Node(-1, LinkStore.ID1_TYPE, 1, 1, 
        UniformDataGenerator.gen(rng, new byte[512], (byte)0, 256));
  }

  @Override
  public void run() {
    logger.info("Requester thread #" + requesterID + " started: will do "
        + nrequests + " ops.");
    logger.debug("Requester thread #" + requesterID + " first random number "
                  + rng.nextLong());
    long starttime = System.currentTimeMillis();
    long endtime = starttime + maxtime * 1000;
    long lastupdate = starttime;
    long curtime = 0;
    long i;

    if (singleAssoc) {
      LinkBenchOp type = LinkBenchOp.UNKNOWN;
      try {
        // add a single assoc to the database
        link.id1 = 45;
        link.id1 = 46;
        type = LinkBenchOp.ADD_LINK;
        addLink(link);

        // read this assoc from the database over and over again
        type = LinkBenchOp.GET_LINK;
        for (i = 0; i < nrequests; i++) {
          boolean found = getLink(link);
          if (found) {
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

    stats.displayStats(Arrays.asList(
        LinkBenchOp.GET_LINK, LinkBenchOp.GET_LINKS_LIST,
        LinkBenchOp.COUNT_LINK,
        LinkBenchOp.UPDATE_LINK, LinkBenchOp.ADD_LINK, 
        LinkBenchOp.RANGE_SIZE, LinkBenchOp.ADD_NODE,
        LinkBenchOp.UPDATE_NODE, LinkBenchOp.DELETE_NODE,
        LinkBenchOp.GET_NODE));
    logger.info("ThreadID = " + requesterID +
                       " total requests = " + i +
                       " requests/second = " + ((1000 * i)/(curtime - starttime)) +
                       " found = " + numfound +
                       " not found = " + numnotfound);

  }

  boolean getLink(Link link) throws Exception {
    return (linkStore.getLink(dbid, link.id1, link.link_type, link.id2) != null ?
            true :
            false);
  }

  Link[] getLinkList(Link link) throws Exception {
    return linkStore.getLinkList(dbid, link.id1, link.link_type);
  }

  long countLinks(Link link) throws Exception {
    return linkStore.countLinks(dbid, link.id1, link.link_type);
  }

  // return a new id2 that satisfies 3 conditions:
  // 1. close to current id2 (can be slightly smaller, equal, or larger);
  // 2. new_id2 % nrequesters = requestersId;
  // 3. smaller or equal to randomid2max unless randomid2max = 0
  private static long fixId2(long id2, long nrequesters,
                             long requesterID, long randomid2max) {

    long newid2 = id2 - (id2 % nrequesters) + requesterID;
    if ((newid2 > randomid2max) && (randomid2max > 0)) newid2 -= nrequesters;
    return newid2;
  }

  void addLink(Link link) throws Exception {
    link.link_type = LinkStore.LINK_TYPE;
    link.id1_type = LinkStore.ID1_TYPE;
    link.id2_type = LinkStore.ID2_TYPE;

    // note that use use write_function here rather than nlinks_func.
    // This is useful if we want request phase to add links in a different
    // manner than load phase.
    long nlinks = LinkBenchLoad.getNlinks(rng,
        link.id1, startid1, maxid1,
        wr_distrfunc, wr_distrconfig, 1);

    // We want to sometimes add a link that already exists and sometimes
    // add a new link. So generate id2 between 0 and 2 * links.
    // unless randomid2max is non-zero (in which case just pick a random id2
    // upto randomid2max). Plus 1 is used to make nlinks atleast 1.
    nlinks = 2 * nlinks + 1;
    link.id2 = (randomid2max == 0 ?
                 (maxid1 + link.id1 + rng.nextInt((int)nlinks + 1)) :
                   rng.nextInt((int)randomid2max));

    if (id2gen_config == 1) {
      link.id2 = fixId2(link.id2, nrequesters, requesterID,
          randomid2max);
    }

    link.visibility = LinkStore.VISIBILITY_DEFAULT;
    link.version = 0;
    link.time = System.currentTimeMillis();

    // generate data as a sequence of random characters from 'a' to 'd'
    link.data = UniformDataGenerator.gen(rng, new byte[datasize],
                                          (byte)'a', 4);

    // no inverses for now
    linkStore.addLink(dbid, link, true);

  }


  void updateLink(Link link) throws Exception {
    link.link_type = LinkStore.LINK_TYPE;


    long nlinks = LinkBenchLoad.getNlinks(rng,
        link.id1, startid1, maxid1,
        nlinks_func, nlinks_config, nlinks_default);

    // id1 is expected to have nlinks links. Update one of those.
    link.id2 = (randomid2max == 0 ?
                 (maxid1 + link.id1 + rng.nextInt((int)nlinks + 1)) :
                   rng.nextInt((int)randomid2max));

    if (id2gen_config == 1) {
      link.id2 = fixId2(link.id2, nrequesters, requesterID,
        randomid2max);
    }

    link.id1_type = LinkStore.ID1_TYPE;
    link.id2_type = LinkStore.ID2_TYPE;
    link.visibility = LinkStore.VISIBILITY_DEFAULT;
    link.version = 0;
    link.time = System.currentTimeMillis();

    // generate data as a sequence of random characters from 'e' to 'h'
    link.data = UniformDataGenerator.gen(rng, new byte[datasize],
                                               (byte)'e', 4);
   

    // no inverses for now
    linkStore.addLink(dbid, link, true);

  }

  void deleteLink(Link link) throws Exception {
    link.link_type = LinkStore.LINK_TYPE;

    long nlinks = LinkBenchLoad.getNlinks(rng,
        link.id1, startid1, maxid1,
        nlinks_func, nlinks_config, nlinks_default);

    // id1 is expected to have nlinks links. Delete one of those.
    link.id2 = (randomid2max == 0 ?
                (maxid1 + link.id1 + rng.nextInt((int)nlinks + 1)) :
                  rng.nextInt((int)randomid2max));

    if (id2gen_config == 1) {
      link.id2 = fixId2(link.id2, nrequesters, requesterID,
        randomid2max);
    }

    // no inverses for now
    linkStore.deleteLink(dbid, link.id1, link.link_type, link.id2,
                     true, // no inverse
                     false);// let us hide rather than delete
  }

  public static class RequestProgress {
    // How many ops before a thread should register its progress
    static final int THREAD_REPORT_INTERVAL = 250;
    // How many ops before a progress update should be printed to console
    private static final int PROGRESS_PRINT_INTERVAL = 10000;
    
    private final Logger progressLogger;
    
    private long totalRequests;
    private final AtomicLong requestsDone;
    
    private long startTime;
    private long timeLimit_s;

    public RequestProgress(Logger progressLogger,
                      long totalRequests, long timeLimit) {
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
      
      long interval = PROGRESS_PRINT_INTERVAL;
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
    long total_requests = Long.parseLong(props.getProperty(Config.NUM_REQUESTS))
                      * Long.parseLong(props.getProperty(Config.NUM_REQUESTERS));
    return new RequestProgress(logger, total_requests,
        Long.parseLong(props.getProperty(Config.MAX_TIME)));
  }
}

