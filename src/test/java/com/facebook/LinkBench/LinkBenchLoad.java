package com.facebook.LinkBench;

import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/*
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
  private Random random_id2; // random number generator for id2 if needed

  private long maxid1;   // max id1 to generate
  private long startid1; // id1 at which to start
  private int  loaderID; // ID for this loader
  private int  nloaders; // #loaders
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

  long linksloaded;
  Random random_data;

  // Random number generators for #links
  Random random_links = new Random();


  public LinkBenchLoad(LinkStore input_store,
                       Properties props,
                       LinkBenchLatency latencyStats,
                       int input_loaderID,
                       int input_nloaders) throws LinkBenchConfigError {
    linksloaded = 0;
    loaderID = input_loaderID;
    this.latencyStats = latencyStats;

    // random number generator for id2 if needed
    randomid2max = Long.parseLong(props.getProperty("randomid2max"));
    random_id2 = (randomid2max > 0) ? (new Random()) : null;

    maxid1 = Long.parseLong(props.getProperty("maxid1"));
    startid1 = Long.parseLong(props.getProperty("startid1"));

    // math functions may cause problems for id1 = 0. Start at 1.
    if (startid1 <= 0) {
      startid1 = 1;
    }

    debuglevel = ConfigUtil.getDebugLevel(props);
    nloaders = input_nloaders;
    store = input_store;
    datasize = Integer.parseInt(props.getProperty("datasize"));

    nlinks_func = Integer.parseInt(props.getProperty("nlinks_func"));
    nlinks_config = Integer.parseInt(props.getProperty("nlinks_config"));
    nlinks_default = Integer.parseInt(props.getProperty("nlinks_default"));
    if (nlinks_func == -2) {//real distribution has it own initialization
      try {
        //load staticical data into RealDistribution
        RealDistribution.loadOneShot(props);
      } catch (Exception e) {
        logger.error(e);
        System.exit(1);
      }
    }

    displayfreq = Long.parseLong(props.getProperty("displayfreq"));
    maxsamples = Integer.parseInt(props.getProperty("maxsamples"));
    stats = new LinkBenchStats(loaderID,
                               displayfreq,
                               maxsamples);

    dbid = props.getProperty("dbid");

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

    long startid;
    long endid;
    long timestart = 0;
    long sameShuffle = 0;
    long diffShuffle = 0;
    boolean singleAssoc = false;

    // special case for single hot row benchmark. If singleAssoc is set, then make
    // this method not print any statistics message, all statistics are collected
    // at a higher layer.
    if (startid1 + 1 == maxid1) {
      startid = startid1;
      endid = startid1 + 1;
      singleAssoc = true;
    } else {
      // partition the range from startid1 to maxid1 into nloaders chunks
      // the last partition could be shorter than others
      long chunksize = (maxid1 - startid1)%nloaders == 0 ?
                     (maxid1 - startid1)/nloaders :
                     (maxid1 - startid1)/nloaders + 1;
      startid = startid1 + (chunksize * loaderID);
      endid = Math.min(startid + chunksize, maxid1); //exclusive
      singleAssoc = false;
    }

    Link link = new Link();
    link.link_type = LinkStore.LINK_TYPE;
    link.id1_type = LinkStore.ID1_TYPE;
    link.id2_type = LinkStore.ID2_TYPE;
    link.visibility = LinkStore.VISIBILITY_DEFAULT;
    link.version = 0;
    link.data = new byte[datasize];
    link.time = System.currentTimeMillis();

    logger.info("Starting loader thread  #" + loaderID +
                " for range [" + startid + ":" + endid + "]");
    
    long prevPercentPrinted = 0;
    for (long id1 = startid; id1 < endid; id1++) {
      long nlinks = 0;
      long newid1 = id1;
      if (nlinks_func == -2) {
        long[] id1_and_newid1 = {id1, id1};
        nlinks = RealDistribution.shuffleAndGetNlinks(id1_and_newid1,
                                                      startid1, maxid1);
        newid1 = id1_and_newid1[1];
        if (newid1 == id1) {
          sameShuffle++;
        } else {
          diffShuffle++;
        }
      } else {
        nlinks = getNlinks(id1, startid1, maxid1,
                  nlinks_func, nlinks_config, nlinks_default);
      }

      if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
        logger.debug("id1 = " + id1 + " newid1 = " + newid1 +
                           " nlinks = " + nlinks);
      }

      for (long j = 0; j < nlinks; j++) {

        link.id1 = newid1;

        // Using random number generator for id2 means we won't know
        // which id2s exist. So link id1 to
        // maxid1 + id1 + 1 thru maxid1 + id1 + nlinks(id1) UNLESS
        // config randomid2max is nonzero.
        if (singleAssoc) {
          link.id2 = 45; // some constant
        } else {
          link.id2 = (randomid2max == 0 ?
                     (maxid1 + newid1 + j) :
                     random_id2.nextInt((int)randomid2max));
          link.time = System.currentTimeMillis();
          // generate data as a sequence of random characters from 'a' to 'd'
          link.data = new byte[datasize];
          for (int k = 0; k < datasize; k++) {
            link.data[k] = (byte)('a' + Math.abs(random_data.nextInt()) % 4);
          }
        }

        if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
          logger.debug("id2 chosen is " + link.id2);
        }
        if (!singleAssoc) {
          timestart = System.nanoTime();
        }

        link.time = System.currentTimeMillis();
        try {
          // no inverses for now
          store.addLink(dbid, link, true);
          linksloaded++;

          if (!singleAssoc && j == nlinks - 1) {
            long timetaken = (System.nanoTime() - timestart);

            // convert to microseconds
            stats.addStats(LinkStore.LOAD_LINK, timetaken/1000, false);

            latencyStats.recordLatency(loaderID, LinkStore.LOAD_LINK, timetaken);
          }

        } catch (Throwable e){//Catch exception if any
            long endtime2 = System.nanoTime();
            long timetaken2 = (endtime2 - timestart)/1000;
            logger.error("Error: " + e.getMessage(), e);
            stats.addStats(LinkStore.LOAD_LINK, timetaken2, true);
            store.clearErrors(loaderID);
            continue;
        }
      }

      if (!singleAssoc) {
        long percent = 100 * (id1 - startid)/(endid - startid);
        if ((percent % 10 == 0) && (percent > prevPercentPrinted)) {
          logger.info("LOADED_ID " + loaderID +
                             ":Percent done = " + percent);
          prevPercentPrinted = percent;
        }
      }
    }
    if (!singleAssoc) {
      logger.info(" Same shuffle = " + sameShuffle +
                         " Different shuffle = " + diffShuffle);
      stats.displayStatsAll();
    }
  }
}
