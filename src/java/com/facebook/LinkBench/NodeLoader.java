package com.facebook.LinkBench;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.facebook.LinkBench.generators.DataGenerator;
import com.facebook.LinkBench.generators.UniformDataGenerator;

/**
 * Load class for generating node data
 * 
 * This is separate from link loading because we can't have multiple parallel
 * loaders loading nodes, as the order of IDs being assigned would be messed up
 * @author tarmstrong
 */
public class NodeLoader implements Runnable {
  private static final long REPORT_INTERVAL = 25000;
  private final Properties props;
  private final Logger logger;
  private final NodeStore nodeStore;
  private final Random rng;
  private final String dbid;

  // Data generation settings
  private final DataGenerator nodeDataGen;
  private final int nodeDataLength;
  
  private final Level debuglevel;
  private final int loaderId;
  private final LinkBenchStats stats;
  private final LinkBenchLatency latencyStats;
  
  private long startTime_ms;
  
  private long nodesLoaded = 0;
  private long totalNodes = 0;
  
  /** Next node count to report on */
  private long nextReport = 0;
  
  public NodeLoader(Properties props, Logger logger,
      NodeStore nodeStore, Random rng,
      LinkBenchLatency latencyStats, int loaderId) {
    super();
    this.props = props;
    this.logger = logger;
    this.nodeStore = nodeStore;
    this.rng = rng;
    this.latencyStats = latencyStats;
    this.loaderId = loaderId;
    
    // TODO: temporary nonsense data
    nodeDataGen = new UniformDataGenerator(Byte.MIN_VALUE, Byte.MAX_VALUE);
    nodeDataLength = 1024;
    
    debuglevel = ConfigUtil.getDebugLevel(props);
    dbid = props.getProperty(Config.DBID);
    

    long displayfreq = Long.parseLong(props.getProperty(Config.DISPLAY_FREQ));
    int maxsamples = Integer.parseInt(props.getProperty(
                                                    Config.MAX_STAT_SAMPLES));
    this.stats = new LinkBenchStats(loaderId, displayfreq, maxsamples);
  }

  @Override
  public void run() {
    logger.info("Starting loader thread  #" + loaderId + " loading nodes");

    try {
      // Set up ids to start at desired range
      nodeStore.resetNodeStore(dbid, 
                    Long.parseLong(props.getProperty(Config.MIN_ID)));
    } catch (Exception e) {
      logger.error("Error while resetting IDs, cannot proceed with " +
      		"node loading", e);
      return;
    }
    
    int bulkLoadBatchSize = nodeStore.bulkLoadBatchSize();
    ArrayList<Node> nodeLoadBuffer = new ArrayList<Node>(bulkLoadBatchSize);

    long maxId = Long.parseLong(props.getProperty(Config.MAX_ID));
    long startId = Long.parseLong(props.getProperty(Config.MIN_ID));
    totalNodes = maxId - startId;
    nextReport = startId + REPORT_INTERVAL;
    startTime_ms = System.currentTimeMillis();
    for (long id = startId; id < maxId; id++) {
      genNode(rng, id, nodeLoadBuffer, bulkLoadBatchSize);
      
    }
    // Load any remaining data
    loadNodes(nodeLoadBuffer);
    
    logger.info("Loading of nodes [" + startId + "," + maxId + ") done");
    stats.displayStats(Arrays.asList(LinkBenchOp.LOAD_NODE_BULK));
  }

  /**
   * Create and insert the node into the DB
   * @param rng
   * @param id1
   */
  private void genNode(Random rng, long id1, ArrayList<Node> nodeLoadBuffer,
                          int bulkLoadBatchSize) {
    Node node = new Node(id1, LinkStore.ID1_TYPE, System.currentTimeMillis(),
                         1, nodeDataGen.fill(rng, new byte[nodeDataLength]));
    nodeLoadBuffer.add(node);
    if (nodeLoadBuffer.size() >= bulkLoadBatchSize) {
      loadNodes(nodeLoadBuffer);
      nodeLoadBuffer.clear();
    }
  }
  
  private void loadNodes(ArrayList<Node> nodeLoadBuffer) {
    long actualIds[] = null;
    long timestart = System.nanoTime();
    try {
      actualIds = nodeStore.bulkAddNodes(dbid, nodeLoadBuffer);
      long timetaken = (System.nanoTime() - timestart);
      nodesLoaded += nodeLoadBuffer.size();
      
      // Check that expected ids were allocated
      assert(actualIds.length == nodeLoadBuffer.size());
      for (int i = 0; i < actualIds.length; i++) {
        if (nodeLoadBuffer.get(i).id != actualIds[i]) { 
          logger.warn("Expected ID of node: " + nodeLoadBuffer.get(i).id + 
                      " != " + actualIds[i] + " the actual ID");
        }
      }
      
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
        		nodesLoaded / timetaken));
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
