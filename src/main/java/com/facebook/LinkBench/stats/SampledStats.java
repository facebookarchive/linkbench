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
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.log4j.Logger;

import com.facebook.LinkBench.ConfigUtil;
import com.facebook.LinkBench.LinkBenchOp;
import com.facebook.LinkBench.LinkStore;


/**
 * This class is used to keep track of statistics.  It collects a sample of the
 * total data (controlled by maxsamples) and can then compute statistics based
 * on that sample.
 *
 * The overall idea is to compute reasonably accurate statistics using bounded
 * space.
 *
 * Currently the class is used to print out stats at given intervals, with the
 * sample taken over a given time interval and printed at the end of the interval.
 */
public class SampledStats {

  // Actual number of operations per type that caller did
  private long numops[];

  // Max samples per type
  private int maxsamples;

  // samples for various optypes
  private long[][] samples;

  /** Number of operations that the sample is drawn from */
  private int opsSinceReset[];

  // minimums encounetered per operation type
  private long minimums[];

  // maximums encountered per operation type
  private long maximums[];

  // #errors encountered per type
  private long errors[];

  // Displayed along with stats
  private int threadID;

  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

  /** Stream to write csv output to ( null if no csv output ) */
  private final PrintStream csvOutput;

  /** Random number generator used to decide which to include in sample */
  private Random rng;

  public SampledStats(int input_threadID,
                        int input_maxsamples, PrintStream csvOutput) {
    threadID = input_threadID;
    maxsamples = input_maxsamples;
    this.csvOutput = csvOutput;
    samples = new long[LinkStore.MAX_OPTYPES][maxsamples];
    opsSinceReset = new int[LinkStore.MAX_OPTYPES];
    minimums = new long[LinkStore.MAX_OPTYPES];
    maximums = new long[LinkStore.MAX_OPTYPES];
    numops = new long[LinkStore.MAX_OPTYPES];
    errors = new long[LinkStore.MAX_OPTYPES];

    rng = new Random();

    csvOutput = null;
  }

  public void addStats(LinkBenchOp type, long timetaken, boolean error) {

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
    int opIndex = opsSinceReset[type.ordinal()];
    opsSinceReset[type.ordinal()]++;

    if (opIndex < maxsamples) {
      samples[type.ordinal()][opIndex] = timetaken;
    } else {
      // Replacing with the probability guarantees that each measurement
      // has an equal probability of being included in the sample
      double pReplace = ((double)maxsamples) / opIndex;
      if (rng.nextDouble() < pReplace) {
        // Select sample to replace randomly
        samples[type.ordinal()][rng.nextInt(maxsamples)] = timetaken;
      }
    }
  }


  public void resetSamples() {
    for (LinkBenchOp type: LinkBenchOp.values()) {
      opsSinceReset[type.ordinal()] = 0;
    }
  }

  /**
   *  display stats for samples from start (inclusive) to end (exclusive)
   * @param type
   * @param start
   * @param end
   * @param startTime_ms
   * @param nowTime_ms
   */
  private void displayStats(LinkBenchOp type, int start, int end,
        long sampleStartTime_ms, long nowTime_ms) {
    int elems = end - start;
    long timestamp = nowTime_ms / 1000;
    long sampleDuration = nowTime_ms - sampleStartTime_ms;

    if (elems <= 0) {
        logger.info("ThreadID = " + threadID +
                         " " + type.displayName() +
                         " totalops = " + numops[type.ordinal()] +
                         " totalErrors = " + errors[type.ordinal()] +
                         " ops = " + opsSinceReset[type.ordinal()] +
                         " sampleDuration = " + sampleDuration + "ms" +
                         " samples = " + elems);
        if (csvOutput != null) {
          csvOutput.println(threadID + "," + timestamp + "," + type.name() +
            "," + numops[type.ordinal()] + "," + errors[type.ordinal()] +
            "," + 0 + "," + sampleDuration +
            ",0,,,,,,,,,");
        }
        return;
    }

    // sort  from start (inclusive) to end (exclusive)
    Arrays.sort(samples[type.ordinal()], start, end);

    RunningMean meanCalc = new RunningMean(samples[type.ordinal()][0]);
    for (int i = start + 1; i < end; i++) {
      meanCalc.addSample(samples[type.ordinal()][i]);
    }

    long min = samples[type.ordinal()][start];
    long p25 = samples[type.ordinal()][start + elems/4];
    long p50 = samples[type.ordinal()][start + elems/2];
    long p75 = samples[type.ordinal()][end - 1 - elems/4];
    long p90 = samples[type.ordinal()][end - 1 - elems/10];
    long p95 = samples[type.ordinal()][end - 1 - elems/20];
    long p99 = samples[type.ordinal()][end - 1 - elems/100];
    long max = samples[type.ordinal()][end - 1];
    double mean = meanCalc.mean();


    DecimalFormat df = new DecimalFormat("#.##");
    logger.info("ThreadID = " + threadID +
                     " " + type.displayName() +
                     " totalOps = " + numops[type.ordinal()] +
                     " totalErrors = " + errors[type.ordinal()] +
                     " ops = " + opsSinceReset[type.ordinal()] +
                     " sampleDuration = " + sampleDuration + "ms" +
                     " samples = " + elems +
                     " mean = " + df.format(mean) +
                     " min = " + min +
                     " 25% = " + p25 +
                     " 50% = " + p50 +
                     " 75% = " + p75 +
                     " 90% = " + p90 +
                     " 95% = " + p95 +
                     " 99% = " + p99 +
                     " max = " + max);
    if (csvOutput != null) {
      csvOutput.println(threadID + "," + timestamp + "," + type.name() +
        "," + numops[type.ordinal()] + "," + errors[type.ordinal()] +
        "," + opsSinceReset[type.ordinal()] + "," + sampleDuration +
        "," + elems + "," + mean + "," + min + "," + p25 + "," + p50 +
        "," + p75 + "," + p90 + "," + p95 + "," + p99 + "," + max);
    }
  }

  /**
   * Write a header with column names for a csv file showing progress
   * @param out
   */
  public static void writeCSVHeader(PrintStream out) {
    out.println("threadID,timestamp,op,totalops,totalerrors,ops," +
            "sampleDuration_us,sampleOps,mean_us,min_us,p25_us,p50_us," +
            "p75_us,p90_us,p95_us,p99_us,max_us");
  }

  public void displayStatsAll(long sampleStartTime_ms, long nowTime_ms) {
    displayStats(sampleStartTime_ms, nowTime_ms,
                 Arrays.asList(LinkBenchOp.values()));
  }

  public void displayStats(long sampleStartTime_ms, long nowTime_ms,
                           Collection<LinkBenchOp> ops) {
    for (LinkBenchOp op: ops) {
      displayStats(op, 0, Math.min(maxsamples, opsSinceReset[op.ordinal()]),
                                   sampleStartTime_ms, nowTime_ms);
    }
  }

  /**
   * @return total operation count so far for type
   */
  public long getCount(LinkBenchOp type) {
    return this.numops[type.ordinal()];
  }

}
