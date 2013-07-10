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
package com.facebook.LinkBench;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.facebook.LinkBench.distributions.LogNormalDistribution;
import com.facebook.LinkBench.generators.DataGenerator;
import com.facebook.LinkBench.stats.LatencyStats;
import com.facebook.LinkBench.stats.SampledStats;
import com.facebook.LinkBench.util.ClassLoadUtil;

/**
 * Load class for generating node data
 *
 * This differs with NodeLoader that it is allowed to run parallel in MR tasks.
 * We do this by load a range of the nodes, However we don't check the
 * return value of the node ID since it won't be continuous.
 */
public class NodeLoaderForMR implements Runnable {
  private static final long REPORT_INTERVAL = 25000;
  private final Properties props;
  private final Logger logger;
  private final NodeStore nodeStore;
  private final Random rng;
  private final String dbid;

  // Data generation settings
  private final DataGenerator nodeDataGen;
  private final LogNormalDistribution nodeDataLength;

  private final Level debuglevel;
  private final int loaderId;
  private final SampledStats stats;
  private final LatencyStats latencyStats;

  private long startTime_ms;

  private long startID;
  private long endID;
  private long nodesLoaded = 0;
  private long totalNodes = 0;

  /** Next node count to report on */
  private long nextReport = 0;


  /** Last time stat update displayed */
  private long lastDisplayTime_ms;

  /** How often to display stat updates */
  private final long displayFreq_ms;


  public NodeLoaderForMR(Properties props, Logger logger,
      NodeStore nodeStore, Random rng,
      LatencyStats latencyStats, PrintStream csvStreamOut, int loaderId,
      long startID, long endID) {
    super();
    this.props = props;
    this.logger = logger;
    this.nodeStore = nodeStore;
    this.rng = rng;
    this.latencyStats = latencyStats;
    this.loaderId = loaderId;
    this.startID = startID;
    this.endID = endID;

    double medianDataLength = ConfigUtil.getDouble(props, Config.NODE_DATASIZE);
    nodeDataLength = new LogNormalDistribution();
    nodeDataLength.init(1, NodeStore.MAX_NODE_DATA, medianDataLength,
                                          Config.NODE_DATASIZE_SIGMA);

    try {
      nodeDataGen = ClassLoadUtil.newInstance(
          ConfigUtil.getPropertyRequired(props, Config.NODE_ADD_DATAGEN),
          DataGenerator.class);
      nodeDataGen.init(props, Config.NODE_ADD_DATAGEN_PREFIX);
    } catch (ClassNotFoundException ex) {
      logger.error(ex);
      throw new LinkBenchConfigError("Error loading data generator class: "
            + ex.getMessage());
    }

    debuglevel = ConfigUtil.getDebugLevel(props);
    dbid = ConfigUtil.getPropertyRequired(props, Config.DBID);


    displayFreq_ms = ConfigUtil.getLong(props, Config.DISPLAY_FREQ) * 1000;
    int maxsamples = ConfigUtil.getInt(props, Config.MAX_STAT_SAMPLES);
    this.stats = new SampledStats(loaderId, maxsamples, csvStreamOut);
  }

  @Override
  public void run() {
    logger.info("Starting loader thread  #" + loaderId + " loading nodes");

    try {
      this.nodeStore.initialize(props, Phase.LOAD, loaderId);
    } catch (Exception e) {
      logger.error("Error while initializing store", e);
      throw new RuntimeException(e);
    }

    try {
      // Set up ids to start at desired range
      // In our case, the node table should don't get truncate
      // just reset the nodeID locally for this loader.
      nodeStore.resetNodeStore(dbid, startID);
    } catch (Exception e) {
      logger.error("Error while resetting IDs, cannot proceed with " +
          "node loading", e);
      return;
    }

    int bulkLoadBatchSize = nodeStore.bulkLoadBatchSize();
    ArrayList<Node> nodeLoadBuffer = new ArrayList<Node>(bulkLoadBatchSize);

    totalNodes = endID - startID;
    nextReport = startID + REPORT_INTERVAL;
    startTime_ms = System.currentTimeMillis();
    lastDisplayTime_ms = startTime_ms;
    for (long id = startID; id < endID; id++) {
      genNode(rng, id, nodeLoadBuffer, bulkLoadBatchSize);

      long now = System.currentTimeMillis();
      if (lastDisplayTime_ms + displayFreq_ms <= now) {
        displayAndResetStats();
      }
    }
    // Load any remaining data
    loadNodes(nodeLoadBuffer);

    logger.info("Loading of nodes [" + startID + "," + endID + ") done");
    displayAndResetStats();
    nodeStore.close();
  }

  private void displayAndResetStats() {
    long now = System.currentTimeMillis();
    stats.displayStats(lastDisplayTime_ms, now,
                       Arrays.asList(LinkBenchOp.LOAD_NODE_BULK));
    stats.resetSamples();
    lastDisplayTime_ms = now;
  }

  /**
   * Create and insert the node into the DB
   * @param rng
   * @param id1
   */
  private void genNode(Random rng, long id1, ArrayList<Node> nodeLoadBuffer,
                          int bulkLoadBatchSize) {
    int dataLength = (int)nodeDataLength.choose(rng);
    Node node = new Node(id1, LinkStore.DEFAULT_NODE_TYPE, 1,
                        (int)(System.currentTimeMillis()/1000),
                        nodeDataGen.fill(rng, new byte[dataLength]));
    nodeLoadBuffer.add(node);
    if (nodeLoadBuffer.size() >= bulkLoadBatchSize) {
      loadNodes(nodeLoadBuffer);
      nodeLoadBuffer.clear();
    }
  }

  private void loadNodes(ArrayList<Node> nodeLoadBuffer) {
    long timestart = System.nanoTime();
    try {
      nodeStore.bulkAddNodes(dbid, nodeLoadBuffer);
      long timetaken = (System.nanoTime() - timestart);
      nodesLoaded += nodeLoadBuffer.size();

      nodeLoadBuffer.clear();

      // convert to microseconds
      stats.addStats(LinkBenchOp.LOAD_NODE_BULK, timetaken/1000, false);
      latencyStats.recordLatency(loaderId,
                    LinkBenchOp.LOAD_NODE_BULK, timetaken);

      if (nodesLoaded >= nextReport) {
        double totalTimeTaken = (System.currentTimeMillis() - startTime_ms) / 1000.0;
        logger.debug(String.format(
            "Loader #%d: %d/%d nodes loaded at %f nodes/sec",
            loaderId, nodesLoaded, totalNodes,
            nodesLoaded / totalTimeTaken));
        nextReport += REPORT_INTERVAL;
      }
    } catch (Throwable e){//Catch exception if any
      long endtime2 = System.nanoTime();
      long timetaken2 = (endtime2 - timestart)/1000;
      logger.error("Error: " + e.getMessage(), e);
      stats.addStats(LinkBenchOp.LOAD_NODE_BULK, timetaken2, true);
      nodeStore.clearErrors(loaderId);
      nodeLoadBuffer.clear();
      return;
    }
  }

  public long getNodesLoaded() {
    return nodesLoaded;
  }
}
