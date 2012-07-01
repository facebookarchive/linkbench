package com.facebook.LinkBench;

import org.apache.log4j.Logger;

import com.facebook.LinkBench.LinkStore.LinkStoreOp;

/**
 * Compute percentile latencies.
 * we record latencies in ms units, upto 100 milliseconds. Any calls
 * that are larger than 100 milliseconds get rolled into 1s and 10 sec
 * latency array.
 */
public class LinkBenchLatency {

  public static int MAX_MILLIS = 100;

  // Number of operations that took less than 2 milliseconds
  // The first index is the threadidm the second index is the type of
  // call and the third index is the 'number of milliseconds'
  // Number of operations between 0 - 100 milliseconds
  private long latencyms[][][];

  // Number of operations between 100ms - 1 sec
  private long latency1s[][];
  
  // Number of operations between 1 - 10 sec
  private long latency10s[][];

  // Number of operations bove 10 seconds
  private long latencyhigh[][];

  // Displayed along with stats
  private int maxThreads;

  // lassort values across threads. The first index is the type of
  // call the second index is the latency in millis.
  long[][] lassort;

  // lassort values across threads. The index is the type of call.
  long[] l1sec;
  long[] l10sec;
  long[] lhigh;

  public LinkBenchLatency(int maxThreads) {
    this.maxThreads = maxThreads;
    latencyms = new long[maxThreads][LinkStore.MAX_OPTYPES][MAX_MILLIS];

    latency1s = new long[maxThreads][LinkStore.MAX_OPTYPES];
    latency10s = new long[maxThreads][LinkStore.MAX_OPTYPES];
    latencyhigh = new long[maxThreads][LinkStore.MAX_OPTYPES];
  }

  /** 
   * Used by the linkbench driver to record latency of each 
   * individual call
   */
  public void recordLatency(int threadid, LinkStoreOp type,
        long nanotimetaken) {

    long timetaken = nanotimetaken/1000; // in milli seconds

    if (timetaken < 100) {
      latencyms[threadid][type.ordinal()][(int)timetaken]++;
    } else if (timetaken < 100000) {
      latency1s[threadid][type.ordinal()]++;
    } else if (timetaken < 1000000) {
      latency10s[threadid][type.ordinal()]++;
    } else {
      latencyhigh[threadid][type.ordinal()]++;
    }
  }

  /**
   * Print out percentile values
   */
  public void displayLatencyStats() {

    l1sec = new long[LinkStore.MAX_OPTYPES];
    l10sec = new long[LinkStore.MAX_OPTYPES];
    lhigh = new long[LinkStore.MAX_OPTYPES];
    lassort = new long[LinkStore.MAX_OPTYPES][MAX_MILLIS];

    // lassort values across threads
    for (int type = 0; type < LinkStore.MAX_OPTYPES; type++) {
      for (int ms = 0; ms < MAX_MILLIS; ms++) {
        for (int i = 0; i < maxThreads; i++) {
          lassort[type][ms] += latencyms[i][type][ms];
        }
      }
    }

    for (int type = 0; type < LinkStore.MAX_OPTYPES; type++) {
      for (int i = 0; i < maxThreads; i++) {
        l1sec[type] += latency1s[i][type];
        l10sec[type] += latency10s[i][type];
        lhigh[type] += latencyhigh[i][type];
      }
    }

    // convert to cumulative sums
    for (int type = 0; type < LinkStore.MAX_OPTYPES; type++) {
      for (int ms = 1; ms < MAX_MILLIS; ms++) {
        lassort[type][ms] += lassort[type][ms-1];
      }
      l1sec[type] += lassort[type][MAX_MILLIS-1];
      l10sec[type] += l1sec[type];
      lhigh[type] += l10sec[type];
    }

    Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
    // print percentiles
    for (LinkStoreOp type: LinkStoreOp.values()) {
      if (lhigh[type.ordinal()] == 0) { // no samples of this type
        continue;
      }

      logger.info(LinkStore.displayName(type) +
                       " p25 = " + getPercentile(type, 25)  + "ms " +
                       " p50 = " + getPercentile(type, 50)  + "ms " +
                       " p75 = " + getPercentile(type, 75)  + "ms " +
                       " p95 = " + getPercentile(type, 95)  + "ms " +
                       " p99 = " + getPercentile(type, 99)  + "ms ");

    }
  }
   
  private long getPercentile(LinkStoreOp type, long percentile) {
    int type_ix = type.ordinal();
    long p = (lhigh[type_ix] * percentile)/100;

    // does this value fall in the millisecond range?
    int ms = 0;
    for (ms = 0; ms < MAX_MILLIS; ms++) {
      if (lassort[type_ix][ms] > p) {
        return ms;
      }
    }

    // The latency is greater than 100 milliseconds.
    if (p < l1sec[type_ix]) {
      ms = 1000;
    } else if (p < l10sec[type_ix]) {
      ms = 10000;
    } else  {
      ms = 100000;
    }
    return ms;
  }
}
