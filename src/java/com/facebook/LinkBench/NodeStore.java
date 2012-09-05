package com.facebook.LinkBench;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Some implementations of NodeStore may require that each
 * dbid be initialized with reset before any data is written to it (so as to
 * ensure that the starting id is actually specified.
 */
public interface NodeStore {
  // Limit data to 1MB
  public static final long MAX_NODE_DATA = 1024 * 1024;
  
  /** initialize the store object */
  public void initialize(Properties p,
      Phase currentPhase, int threadId) throws IOException, Exception;
  
  /**
   * Reset node storage to a clean state in shard:
   *   deletes all stored nodes
   *   resets id allocation, with new IDs to be allocated starting from startID
   */
  public void resetNodeStore(String dbid, long startID) throws Exception;
  
  /**
   * Adds a new node object to the database.
   * 
   * This allocates a new id for the object and returns i
   * @param dbid the db shard to put that object in
   * @param node a node with all data aside from id filled in.  The id
   *    field is *not* updated to the new value by this function
   * @return the id allocated for the node
   */
  public long addNode(String dbid, Node node) throws Exception;
  
  /**
   * Bulk loading to more efficiently load nodes
   * @param dbid
   * @param nodes
   * @return the actual IDs allocated to the nodes
   * @throws Exception
   */
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception;
  
  /** 
   * Preferred size of data to load
   * @return
   */
  public int bulkLoadBatchSize();
  
  /**
   * Get a node of the specified type
   * @param dbid the db shard the id is mapped to
   * @param type the type of the object
   * @param id the id of the object
   * @return null if not found, a Node with all fields filled in otherwise
   */
  public Node getNode(String dbid, int type, long id) throws Exception;
  
  /**
   * Update all parameters of the node specified.  
   * @param dbid
   * @param node
   * @return true if the update was successful, false if not present
   */
  public boolean updateNode(String dbid, Node node) throws Exception;
  
  /**
   * Delete the object specified by the arguments 
   * @param dbid
   * @param type
   * @param id
   * @return true if the node was deleted, false if not present
   */
  public boolean deleteNode(String dbid, int type, long id) throws Exception;

  public void clearErrors(int loaderId);
}
