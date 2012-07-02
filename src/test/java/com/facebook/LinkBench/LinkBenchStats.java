package com.facebook.LinkBench;


import java.util.Arrays;
import java.util.Collection;

import org.apache.log4j.Logger;

import com.facebook.LinkBench.LinkStore.LinkStoreOp;

// Compute statistics
public class LinkBenchStats {

  // Actual number of operations per type that caller did
  private long numops[];

  // Max samples per type
  private int maxsamples;

  // samples for various optypes
  private long[][] samples;

  // actual number of samples taken for various types
  private int samplestaken[];

  // minimums encounetered per operation type
  private long minimums[];

  // maximums encountered per operation type
  private long maximums[];

  // #errors encountered per type
  private long errors[];

  // stats displayed after this many seconds
  private long displayfreq;

  // time at which we displayed stats last for each op type
  private long lastdisplaytime[];

  // Displayed along with stats
  private int threadID;
  
  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

  public LinkBenchStats(int input_threadID,
                        long input_displayfreq,
                        int input_maxsamples) {
    threadID = input_threadID;
    displayfreq = input_displayfreq;
    maxsamples = input_maxsamples;
    samples = new long[LinkStore.MAX_OPTYPES][maxsamples];
    samplestaken = new int[LinkStore.MAX_OPTYPES];
    minimums = new long[LinkStore.MAX_OPTYPES];
    maximums = new long[LinkStore.MAX_OPTYPES];
    numops = new long[LinkStore.MAX_OPTYPES];
    errors = new long[LinkStore.MAX_OPTYPES];
    lastdisplaytime = new long[LinkStore.MAX_OPTYPES];
    long timenow = System.currentTimeMillis();
    for (int i = 0; i < LinkStore.MAX_OPTYPES; i++) {
      lastdisplaytime[i] = timenow;
    }
  }

  public void addStats(LinkStoreOp type, long timetaken, boolean error) {

    if (error) {
      errors[type.ordinal()]++;
    }

    if ((minimums[type.ordinal()] == 0) || (minimums[type.ordinal()] > timetaken)) {
      minimums[type.ordinal()] = timetaken;
    }

    if (timetaken > maximums[type.ordinal()]) {
      maximums[type.ordinal()] = timetaken;
    }

    numops[type.ordinal()]++;
    int index = samplestaken[type.ordinal()];

    if (index < maxsamples) {
      samples[type.ordinal()][index] = timetaken;
      samplestaken[type.ordinal()]++;
      index++;
    }

    long timenow = System.currentTimeMillis();
    if ((timenow - lastdisplaytime[type.ordinal()]) > displayfreq * 1000) {
      displayStats(type, 0, index);
      samplestaken[type.ordinal()] = 0;
      lastdisplaytime[type.ordinal()] = timenow;
    }

  }


  // display stats for samples from start (inclusive) to end (exclusive)
  private void displayStats(LinkStoreOp type, int start, int end) {
    int elems = end - start;

    if (elems <= 0) {
        logger.info("ThreadID = " + threadID +
                         " " + LinkStore.displayName(type) +
                         " numops = " + numops[type.ordinal()] +
                         " errors = " + errors[type.ordinal()] +
                         " samples = " + elems);
        return;
    }

    // sort  from start (inclusive) to end (exclusive)
    Arrays.sort(samples[type.ordinal()], start, end);

    logger.info("ThreadID = " + threadID +
                     " " + LinkStore.displayName(type) +
                     " totalops = " + numops[type.ordinal()] +
                     " totalerrors = " + errors[type.ordinal()] +
                     " ops = " + elems +
                     " min = " + samples[type.ordinal()][start] +
                     " 25% = " + samples[type.ordinal()][start + elems/4] +
                     " 50% = " + samples[type.ordinal()][start + elems/2] +
                     " 75% = " + samples[type.ordinal()][end - 1 - elems/4] +
                     " 90% = " + samples[type.ordinal()][end - 1 - elems/10] +
                     " 95% = " + samples[type.ordinal()][end - 1 - elems/20] +
                     " 99% = " + samples[type.ordinal()][end - 1 - elems/100] +
                     " max = " + samples[type.ordinal()][end - 1]);

  }

  public void displayStatsAll() {
    displayStats(Arrays.asList(LinkStoreOp.values()));
  }
  
  public void displayStats(Collection<LinkStoreOp> ops) {
    for (LinkStoreOp op: ops) {
      displayStats(op, 0, samplestaken[op.ordinal()]);
    }
  }

}
