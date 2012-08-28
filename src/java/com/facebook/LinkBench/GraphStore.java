package com.facebook.LinkBench;

import java.util.List;

/**
 * An abstract class for storing both nodes and edges 
 * @author tarmstrong
 */
public abstract class GraphStore extends LinkStore implements NodeStore {

  /** Provide generic implementation */
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
    long ids[] = new long[nodes.size()];
    int i = 0;
    for (Node node: nodes) {
      long id = addNode(dbid, node);
      ids[i++] = id;
    }
    return ids;
  }
}
