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

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.facebook.LinkBench.LinkBenchRequest.RequestProgress;
import com.facebook.LinkBench.distributions.AccessDistributions.AccessDistMode;
import com.facebook.LinkBench.distributions.UniformDistribution;
import com.facebook.LinkBench.generators.UniformDataGenerator;
import com.facebook.LinkBench.stats.LatencyStats;

public abstract class GraphStoreTestBase extends TestCase {

  protected String testDB = "linkbench_unittestdb";
  private Logger logger = Logger.getLogger("");

  /**
   * Reinitialize link store database properties.
   * Should attempt to clean database
   * @param props Properties for test DB.
   *        Override any required properties in this property dict
   */
  protected abstract void initStore(Properties props)
                                    throws IOException, Exception;

  /**
   * Override to vary size of test
   * @return number of ids to use in testing
   */
  protected long getIDCount() {
    return 50000;
  }

  /**
   * Override to vary number of requests in test
   */
  protected int getRequestCount() {
    return 100000;
  }

  /**
   * Override to vary maximum number of threads
   */
  protected int maxConcurrentThreads() {
    return Integer.MAX_VALUE;
  }

  /** Get a new handle to the initialized store, wrapped in
   * DummyLinkStore
   * @return new handle to linkstore
   */
  protected abstract DummyLinkStore getStoreHandle(boolean initialized)
                                    throws IOException, Exception;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    initStore(basicProps());
  }

  /**
   * Provide properties for basic test store
   * @return
   */
  protected Properties basicProps() {
    Properties props = new Properties();
    props.setProperty(Config.DBID, testDB);
    return props;
  }


  public static void fillLoadProps(Properties props, long startId, long idCount,
      int linksPerId) {
    LinkStoreTestBase.fillLoadProps(props, startId, idCount, linksPerId);
    props.setProperty(Config.NODE_DATASIZE, "512.0");
    props.setProperty(Config.NODE_ADD_DATAGEN,
                      UniformDataGenerator.class.getName());
    props.setProperty(Config.NODE_ADD_DATAGEN_PREFIX +
                      Config.UNIFORM_GEN_STARTBYTE, "0");
    props.setProperty(Config.NODE_ADD_DATAGEN_PREFIX +
                      Config.UNIFORM_GEN_ENDBYTE, "255");
  }

  public static void fillReqProps(Properties props, long startId, long idCount,
      int requests, long timeLimit, double p_addlink, double p_deletelink,
      double p_updatelink, double p_countlink, double p_multigetlink,
      double p_getlinklist, double p_addnode, double p_updatenode,
      double p_deletenode, double p_getnode) {
    LinkStoreTestBase.fillReqProps(props, startId, idCount, requests, timeLimit,
        p_addlink, p_deletelink, p_updatelink,
        p_countlink, p_multigetlink,
        p_getlinklist, true);
    props.setProperty(Config.PR_ADD_NODE, Double.toString(p_addnode));
    props.setProperty(Config.PR_UPDATE_NODE, Double.toString(p_updatenode));
    props.setProperty(Config.PR_DELETE_NODE, Double.toString(p_deletenode));
    props.setProperty(Config.PR_GET_NODE, Double.toString(p_getnode));

    props.setProperty(Config.NODE_READ_CONFIG_PREFIX +
          Config.ACCESS_FUNCTION_SUFFIX, UniformDistribution.class.getName());
    props.setProperty(Config.NODE_UPDATE_CONFIG_PREFIX +
        Config.ACCESS_FUNCTION_SUFFIX, AccessDistMode.ROUND_ROBIN.name());
    props.setProperty(Config.NODE_UPDATE_CONFIG_PREFIX +
        Config.ACCESS_CONFIG_SUFFIX, "0");
    props.setProperty(Config.NODE_DELETE_CONFIG_PREFIX +
        Config.ACCESS_FUNCTION_SUFFIX, UniformDistribution.class.getName());

    props.setProperty(Config.NODE_DATASIZE, "1024");
    props.setProperty(Config.NODE_ADD_DATAGEN,
                      UniformDataGenerator.class.getName());
    props.setProperty(Config.NODE_ADD_DATAGEN_PREFIX +
                      Config.UNIFORM_GEN_STARTBYTE, "0");
    props.setProperty(Config.NODE_ADD_DATAGEN_PREFIX +
                      Config.UNIFORM_GEN_ENDBYTE, "255");

    props.setProperty(Config.NODE_DATASIZE, "1024");
    props.setProperty(Config.NODE_UP_DATAGEN,
                      UniformDataGenerator.class.getName());
    props.setProperty(Config.NODE_UP_DATAGEN_PREFIX +
                      Config.UNIFORM_GEN_STARTBYTE, "0");
    props.setProperty(Config.NODE_UP_DATAGEN_PREFIX +
                      Config.UNIFORM_GEN_ENDBYTE, "255");
  }


  /**
   * Test the full workload with node and link ops to exercise the
   * requester
   * @throws Exception
   * @throws IOException
   */
  @Test
  public void testFullWorkload() throws IOException, Exception {
    long startId = 532;
    long idCount = getIDCount();
    int linksPerId = 5;

    int requests = getRequestCount();
    long timeLimit = requests;


    Properties props = basicProps();
    fillLoadProps(props, startId, idCount, linksPerId);

    double p_add = 0.1, p_del = 0.05, p_up = 0.05, p_count = 0.05,
           p_multiget = 0.05, p_getlinks = 0.1,
           p_add_node = 0.2, p_up_node = 0.05,
           p_del_node = 0.05, p_get_node = 0.3;
    fillReqProps(props, startId, idCount, requests, timeLimit,
        p_add * 100, p_del * 100, p_up * 100, p_count * 100, p_multiget * 100,
        p_getlinks * 100, p_add_node * 100, p_up_node * 100,
        p_del_node * 100, p_get_node * 100);

    try {
      Random rng = LinkStoreTestBase.createRNG();

      LinkStoreTestBase.serialLoad(rng, logger, props, getStoreHandle(false));
      serialLoadNodes(rng, logger, props, getStoreHandle(false));

      DummyLinkStore reqStore = getStoreHandle(false);
      LatencyStats latencyStats = new LatencyStats(1);
      RequestProgress tracker = new RequestProgress(logger, requests, timeLimit, 1, 10000);

      // Test both link and node requests
      LinkBenchRequest requester = new LinkBenchRequest(reqStore,
               reqStore, props, latencyStats, System.out, tracker, rng, 0, 1);
      tracker.startTimer();

      requester.run();

      latencyStats.displayLatencyStats();
      latencyStats.printCSVStats(System.out, true);

      assertEquals(requests, reqStore.adds + reqStore.updates + reqStore.deletes +
          reqStore.countLinks + reqStore.multigetLinks + reqStore.getLinkLists +
          reqStore.addNodes + reqStore.updateNodes + reqStore.deleteNodes +
          reqStore.getNodes);
      // Check that the proportion of operations is roughly right - within 1%
      // For now, updates are actually implemented as add operations
      assertTrue(Math.abs(reqStore.adds / (double)requests -
          (p_add + p_up)) < 0.01);
      assertTrue(Math.abs(reqStore.updates /
                      (double)requests - 0.0) < 0.01);
      assertTrue(Math.abs(reqStore.deletes /
                       (double)requests - p_del) < 0.01);
      assertTrue(Math.abs(reqStore.countLinks /
                       (double)requests - p_count) < 0.01);
      assertTrue(Math.abs(reqStore.multigetLinks /
                       (double)requests - p_multiget) < 0.01);
      assertTrue(Math.abs(reqStore.getLinkLists /
                       (double)requests - p_getlinks) < 0.01);
      assertTrue(Math.abs(reqStore.addNodes /
                      (double)requests - p_add_node) < 0.01);
      assertTrue(Math.abs(reqStore.updateNodes /
                      (double)requests - p_up_node) < 0.01);
      assertTrue(Math.abs(reqStore.deleteNodes /
                      (double)requests - p_del_node) < 0.01);
      assertTrue(Math.abs(reqStore.getNodes /
                      (double)requests - p_get_node) < 0.01);

      assertEquals(0, reqStore.bulkLoadCountOps);
      assertEquals(0, reqStore.bulkLoadLinkOps);
    } finally {
      try {
        LinkStoreTestBase.deleteIDRange(testDB, getStoreHandle(true),
                                        startId, idCount);
        deleteNodeIDRange(testDB, LinkStore.DEFAULT_NODE_TYPE,
                          getStoreHandle(true), startId, idCount);
      } catch (Throwable t) {
        System.err.println("Error during cleanup:");
        t.printStackTrace();
      }
    }
  }

  /**
   * Delete all nodes in ID range specified
   */
  static void deleteNodeIDRange(String testDB, int type,
      DummyLinkStore storeHandle,
      long startId, long idCount) throws Exception {
    for (long i = startId; i < startId + idCount; i++) {
      storeHandle.deleteNode(testDB, type, i);
    }
  }

  private void serialLoadNodes(Random rng, Logger logger, Properties props,
      DummyLinkStore storeHandle) throws Exception {
    storeHandle.initialize(props, Phase.LOAD, 0);
    storeHandle.resetNodeStore(testDB, ConfigUtil.getLong(props,
                                                  Config.MIN_ID));
    storeHandle.close(); // Close before passing to loader

    LatencyStats stats = new LatencyStats(1);
    NodeLoader loader = new NodeLoader(props, logger, storeHandle, rng,
                                       stats, System.out, 0);
    loader.run();
  }
}
