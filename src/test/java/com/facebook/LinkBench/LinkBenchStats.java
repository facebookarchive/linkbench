package com.facebook.LinkBench;


import java.util.Arrays;

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

  public void addStats(int type, long timetaken, boolean error) {

    if (error) {
      errors[type]++;
    }

    if ((minimums[type] == 0) || (minimums[type] > timetaken)) {
      minimums[type] = timetaken;
    }

    if (timetaken > maximums[type]) {
      maximums[type] = timetaken;
    }

    numops[type]++;
    int index = samplestaken[type];

    if (index < maxsamples) {
      samples[type][index] = timetaken;
      samplestaken[type]++;
      index++;
    }

    long timenow = System.currentTimeMillis();
    if ((timenow - lastdisplaytime[type]) > displayfreq * 1000) {
      displayStats(type, 0, index);
      samplestaken[type] = 0;
      lastdisplaytime[type] = timenow;
    }

  }


  // display stats for samples from start (inclusive) to end (exclusive)
  private void displayStats(int type, int start, int end) {
    int elems = end - start;

    if (elems <= 0) {
        System.out.println("ThreadID = " + threadID +
                           " " + LinkStore.displaynames[type] +
                           " numops = " + numops[type] +
                           " errors = " + errors[type] +
                           " samples = " + elems);
        return;
    }

    // sort  from start (inclusive) to end (exclusive)
    Arrays.sort(samples[type], start, end);

    System.out.println("ThreadID = " + threadID +
                       " " + LinkStore.displaynames[type] +
                       " totalops = " + numops[type] +
                       " totalerrors = " + errors[type] +
                       " ops = " + elems +
                       " min = " + samples[type][start] +
                       " 25% = " + samples[type][start + elems/4] +
                       " 50% = " + samples[type][start + elems/2] +
                       " 75% = " + samples[type][end - 1 - elems/4] +
                       " 90% = " + samples[type][end - 1 - elems/10] +
                       " 95% = " + samples[type][end - 1 - elems/20] +
                       " 99% = " + samples[type][end - 1 - elems/100] +
                       " max = " + samples[type][end - 1]);

  }

  public void displayStatsAll() {
    displayStats(LinkStore.ADD_LINK, 0, samplestaken[LinkStore.ADD_LINK]);
    displayStats(LinkStore.DELETE_LINK, 0, samplestaken[LinkStore.DELETE_LINK]);
    displayStats(LinkStore.UPDATE_LINK, 0, samplestaken[LinkStore.UPDATE_LINK]);
    displayStats(LinkStore.COUNT_LINK, 0, samplestaken[LinkStore.COUNT_LINK]);
    displayStats(LinkStore.GET_LINK, 0, samplestaken[LinkStore.GET_LINK]);
    displayStats(LinkStore.GET_LINKS_LIST, 0, samplestaken[LinkStore.GET_LINKS_LIST]);
    displayStats(LinkStore.LOAD_LINK, 0, samplestaken[LinkStore.LOAD_LINK]);
    displayStats(LinkStore.UNKNOWN, 0, samplestaken[LinkStore.UNKNOWN]);
    displayStats(LinkStore.RANGE_SIZE, 0, samplestaken[LinkStore.RANGE_SIZE]);
  }

}
