/*
 * Copyright 2012, Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.LinkBench.stats;

import java.io.PrintStream;
import java.text.DecimalFormat;

import com.facebook.LinkBench.store.LinkStore;
import org.apache.log4j.Logger;

import com.facebook.LinkBench.ConfigUtil;
import com.facebook.LinkBench.LinkBenchOp;
import com.facebook.LinkBench.store.LinkStore;


/**
 * Class used to track and compute latency statistics, particularly
 * percentiles. Times are divided into buckets, with counts maintained
 * per bucket.  The division into buckets is based on typical latencies
 * for database operations: most are in the range of 0.1ms to 100ms.
 * we have 0.1ms-granularity buckets up to 1ms, then 1ms-granularity from
 * 1-100ms, then 100ms-granularity, and then 1s-granularity.
 */
public class LatencyStats {

  public static int MAX_MILLIS = 100;

  /**
   * Keep track of running mean per thread and op type
   */
  private RunningMean means[][];

  /** Final means per op type */
  double finalMeans[];

  // Displayed along with stats
  private int maxThreads;

  public LatencyStats(int maxThreads) {
    this.maxThreads = maxThreads;
    means = new RunningMean[maxThreads][LinkStore.MAX_OPTYPES];
    bucketCounts = new long[maxThreads][LinkStore.MAX_OPTYPES][NUM_BUCKETS];
    maxLatency = new long[maxThreads][LinkStore.MAX_OPTYPES];
  }


  private static final int SUB_MS_BUCKETS = 10; // Sub-ms granularity
  private static final int MS_BUCKETS = 99; // ms-granularity buckets
  private static final int HUNDREDMS_BUCKETS = 9; // 100ms-granularity buckets
  private static final int SEC_BUCKETS = 9; // 1s-granularity buckets
  public static final int NUM_BUCKETS = SUB_MS_BUCKETS + MS_BUCKETS +
          HUNDREDMS_BUCKETS + SEC_BUCKETS + 1;

  /** Counts of operations falling into each bucket */
  private final long bucketCounts[][][];

  /** Counts of samples per type */
  private long sampleCounts[];

  /** Cumulative bucket counts keyed by type, bucket# (calculated at end) */
  private long bucketCountsCumulative[][];

  /** Maximum latency by thread and type */
  private long maxLatency[][];

  public static int latencyToBucket(long microTime) {
    long ms = 1000;
    long msTime = microTime / ms; // Floored
    if (msTime == 0) {
      // Bucket per 0.1 ms
      return (int) (microTime / 100);
    } else if (msTime < 100) {
      // msBucket = 0 means 1-2 ms
      int msBucket = (int) msTime - 1;
      // Bucket per ms
      return SUB_MS_BUCKETS + msBucket;
    } else if (msTime < 1000){
      int hundredMSBucket = (int) ( msTime / 100 ) - 1;
      return SUB_MS_BUCKETS + MS_BUCKETS + hundredMSBucket;
    } else if (msTime < 10000) {
      int secBucket = (int) (msTime / 1000) - 1;
      return SUB_MS_BUCKETS + MS_BUCKETS + HUNDREDMS_BUCKETS + secBucket;
    } else {
      return NUM_BUCKETS - 1;
    }
  }

  /**
   *
   * @param bucket
   * @return inclusive min and exclusive max time in microsecs for bucket
   */
  public static long[] bucketBound(int bucket) {
    int ms = 1000;
    long s = ms * 1000;
    long res[] = new long[2];
    if (bucket < SUB_MS_BUCKETS) {
      res[0] = bucket * 100;
      res[1] = (bucket+1) * 100;
    } else if (bucket < SUB_MS_BUCKETS + MS_BUCKETS) {
      res[0] = (bucket - SUB_MS_BUCKETS + 1) * ms;
      res[1] = (bucket - SUB_MS_BUCKETS + 2) * ms;
    } else if (bucket < SUB_MS_BUCKETS + MS_BUCKETS + HUNDREDMS_BUCKETS) {
      int hundredMS = bucket - SUB_MS_BUCKETS - MS_BUCKETS + 1;
      res[0] = hundredMS * 100 * ms;
      res[1] = (hundredMS + 1) * 100 * ms;
    } else if (bucket < SUB_MS_BUCKETS + MS_BUCKETS + HUNDREDMS_BUCKETS + SEC_BUCKETS) {
      int secBucket = bucket - SUB_MS_BUCKETS - MS_BUCKETS - SEC_BUCKETS + 1;
      res[0] = secBucket * s;
      res[1] = (secBucket + 1) * s;
    } else {
      res[0] = (SEC_BUCKETS + 1)* s;
      res[1] = 100 * s;
    }
    return res;
  }


  /**
   * Used by the linkbench driver to record latency of each
   * individual call
   */
  public void recordLatency(int threadid, LinkBenchOp type,
        long microtimetaken) {
    long opBuckets[] = bucketCounts[threadid][type.ordinal()];
    int bucket = latencyToBucket(microtimetaken);
    opBuckets[bucket]++;

    double time_ms = microtimetaken / 1000.0;
    if (means[threadid][type.ordinal()] == null) {
      means[threadid][type.ordinal()] = new RunningMean(time_ms);
    } else {
      means[threadid][type.ordinal()].addSample(time_ms);
    }

    if (maxLatency[threadid][type.ordinal()] < microtimetaken) {
      maxLatency[threadid][type.ordinal()] = microtimetaken;
    }
  }

  /**
   * Print out percentile values
   */
  public void displayLatencyStats() {
    calcMeans();
    calcCumulativeBuckets();

    Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
    // print percentiles
    for (LinkBenchOp type: LinkBenchOp.values()) {
      if (sampleCounts[type.ordinal()] == 0) { // no samples of this type
        continue;
      }

      DecimalFormat df = new DecimalFormat("#.###"); // Format to max 3 decimal place
      logger.info(type.displayName() +
                     " count = " + sampleCounts[type.ordinal()] + " " +
                     " p25 = " + percentileString(type, 25)  + "ms " +
                     " p50 = " + percentileString(type, 50)  + "ms " +
                     " p75 = " + percentileString(type, 75)  + "ms " +
                     " p95 = " + percentileString(type, 95)  + "ms " +
                     " p99 = " + percentileString(type, 99)  + "ms " +
                     " max = " + df.format(getMax(type)) + "ms " +
                     " mean = " + df.format(getMean(type))+ "ms");
    }
  }

  public void printCSVStats(PrintStream out, boolean header) {
    printCSVStats(out, header, LinkBenchOp.values());
  }

  public void printCSVStats(PrintStream out, boolean header, LinkBenchOp... ops) {
    int percentiles[] = new int[] {25, 50, 75, 95, 99};

    // Write out the header
    if (header) {
      out.print("op,count");
      for (int percentile: percentiles) {
        out.print(String.format(",p%d_low,p%d_high", percentile, percentile));
      }
      out.print(",max,mean");
      out.println();
    }

    // Print in milliseconds down to 10us granularity
    DecimalFormat df = new DecimalFormat("#.##");

    for (LinkBenchOp op: ops) {
      long samples = sampleCounts[op.ordinal()];
      if (samples == 0) {
        continue;
      }
      out.print(op.name());
      out.print(",");
      out.print(samples);

      for (int percentile: percentiles) {
        long bounds[] = getBucketBounds(op, percentile);
        out.print(",");
        out.print(df.format(bounds[0] / 1000.0));
        out.print(",");
        out.print(df.format(bounds[1] / 1000.0));
      }

      out.print(",");
      out.print(df.format(getMax(op)));
      out.print(",");
      out.print(df.format(getMean(op)));
      out.println();
    }
  }

  /**
   * Fill in the counts and means arrays
   */
  private void calcMeans() {
    sampleCounts = new long[LinkStore.MAX_OPTYPES];
    finalMeans = new double[LinkStore.MAX_OPTYPES];
    for (int i = 0; i < LinkStore.MAX_OPTYPES; i++) {
      long samples = 0;
      for (int thread = 0; thread < maxThreads; thread++) {
        if (means[thread][i] != null) {
          samples += means[thread][i].samples();
        }
      }
      sampleCounts[i] = samples;

      double weightedMean = 0.0;
      for (int thread = 0; thread < maxThreads; thread++) {
        if (means[thread][i] != null) {
          weightedMean += (means[thread][i].samples() / (double) samples) *
                           means[thread][i].mean();
        }
      }
      finalMeans[i] = weightedMean;
    }

  }

  private void calcCumulativeBuckets() {
    // Calculate the cumulative operation counts by bucket for each type
    bucketCountsCumulative = new long[LinkStore.MAX_OPTYPES][NUM_BUCKETS];

    for (int type = 0; type < LinkStore.MAX_OPTYPES; type++) {
      long count = 0;
      for (int bucket = 0; bucket < NUM_BUCKETS; bucket++) {
        for (int thread = 0; thread < maxThreads; thread++) {
          count += bucketCounts[thread][type][bucket];
        }
        bucketCountsCumulative[type][bucket] = count;
      }
    }
  }

  private long[] getBucketBounds(LinkBenchOp type, long percentile) {
    long n = sampleCounts[type.ordinal()];
    // neededRank is the rank of the sample at the desired percentile
    long neededRank = (long) ((percentile / 100.0) * n);

    int bucketNum = -1;
    for (int i = 0; i < NUM_BUCKETS; i++) {
      long rank = bucketCountsCumulative[type.ordinal()][i];
      if (neededRank <= rank) {
        // We have found the right bucket
        bucketNum = i;
        break;
      }
    }
    assert(bucketNum >= 0); // Should definitely be found;


    return bucketBound(bucketNum);
  }


  /**
   *
   * @return A human-readable string for the bucket bounds
   */
  private String percentileString(LinkBenchOp type, long percentile) {
    return boundsToString(getBucketBounds(type, percentile));
  }

  static String boundsToString(long[] bucketBounds) {
    double minMs = bucketBounds[0] / 1000.0;
    double maxMs = bucketBounds[1] / 1000.0;

    DecimalFormat df = new DecimalFormat("#.##"); // Format to max 1 decimal place
    return "["+ df.format(minMs) + "," + df.format(maxMs) + "]";
  }

  private double getMean(LinkBenchOp type) {
    return finalMeans[type.ordinal()];
  }

  private double getMax(LinkBenchOp type) {
    long max_us = 0;
    for (int thread = 0; thread < maxThreads; thread++) {
      max_us = Math.max(max_us, maxLatency[thread][type.ordinal()]);
    }
    return max_us / 1000.0;
  }
}
