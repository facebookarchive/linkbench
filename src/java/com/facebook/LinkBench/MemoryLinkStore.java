package com.facebook.LinkBench;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Simple in-memory implementation of GraphStore
 * Not efficient or optimized at all, just for testing purposes.
 * 
 * MemoryLinkStore instances sharing the same data can be created
 * using the newInstance() method.
 * MemoryLinkStore can be accessed concurrently from multiple threads,
 * but a simple mutex is used so there is no internal concurrency (requests
 * are serialized)
 */
public class MemoryLinkStore extends GraphStore {
  private static class LinkLookupKey {
    final long id1;
    final long link_type;
    
    public LinkLookupKey(long id1, long link_type) {
      super();
      this.id1 = id1;
      this.link_type = link_type;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof LinkLookupKey)) {
        return false;
      }
      LinkLookupKey other2 = (LinkLookupKey)other; 
      return id1 == other2.id1 && link_type == other2.link_type;
    }

    @Override
    public int hashCode() {
      return Long.valueOf(id1).hashCode() ^ Long.valueOf(link_type).hashCode();
    }
  }
  
  /** Order links from most to least recent */
  private static class LinkTimeStampComparator implements Comparator<Link> {

    @Override
    public int compare(Link l1, Link l2) {
      // ascending order of id1
      if (l1.id1 != l2.id1) {
        if (l1.id1 < l2.id2) {
          return -1; 
        } else {
          return 1;
        }
      }
      if (l1.time != l2.time) {
        // descending order of time
        if (l1.time < l2.time) {
          return 1; 
        } else {
          return -1;
        }
      }
      
      // ascending order of id2
      if (l1.id2 == l2.id2) {
        return 0;
      } else if (l1.id2 < l2.id2) {
        return -1;
      } else {
        return 1;
      }
    }
  }
  
  /**
   * Class for allocating IDs and storing objects
   */
  private static class NodeDB {
    private long nextID; // Next id to allocate
    Map<Long, Node> data = new HashMap<Long, Node>();
    
    /** Construct a new instance allocating ids from 1 */
    NodeDB() {
      this(1);
    }
    
    NodeDB(long startID) {
      this.nextID = startID;
    }
    
    long allocateID() {
      return nextID++;
    }
    
    void reset(long startID) {
      nextID = startID;
      data.clear();
    }
  }
  
  
  /** Simple implementation of LinkStore with nested maps and a set of
   * links sorted by timestamp: 
   * dbid ->  (id1, assoc_type) -> links */
  private final Map<String, Map<LinkLookupKey, SortedSet<Link>>> linkdbs;
  
  private final Map<String, NodeDB> nodedbs;
  
  /**
   * Storage for objects
   */

  private static final Comparator<Link> LINK_COMPARATOR = new LinkTimeStampComparator();
  
  /** 
   * Create a new MemoryLinkStore instance with fresh data 
   */
  public MemoryLinkStore() {
    this(new HashMap<String, Map<LinkLookupKey, SortedSet<Link>>>(),
         new HashMap<String, NodeDB>());
  }
  
  /** 
   * Create a new MemoryLinkStore handle sharing data with existing instance 
   */
  private MemoryLinkStore(Map<String, Map<LinkLookupKey, SortedSet<Link>>> linkdbs,
                          Map<String, NodeDB> nodedbs) {
    this.linkdbs = linkdbs;
    this.nodedbs = nodedbs;
  }
  
  /**
   * Find a list of links based on 
   * @param dbid
   * @param id1
   * @param link_type
   * @param createPath if true, create nested structures.  If false, return
   *                  null if map for (dbid, id, link_type) does not exist and
   *                  do not modify the structure;
   * @return
   */
  private SortedSet<Link> findLinkByKey(String dbid, long id1,
                                  long link_type, boolean createPath) {
    Map<LinkLookupKey, SortedSet<Link>> db = linkdbs.get(dbid);
    if (db == null) {
      if (createPath) {
        // Autocreate db
        db = new HashMap<LinkLookupKey, SortedSet<Link>>();
        linkdbs.put(dbid, db);
      } else {
        return null;
      }
    }
    LinkLookupKey key = new LinkLookupKey(id1, link_type);
    SortedSet<Link> links = db.get(key);
    if (links == null) {
      if (createPath) {
        links = newSortedLinkSet();
        db.put(key, links);
      } else {
        return null;
      }
    }
    return links;
  }

  private TreeSet<Link> newSortedLinkSet() {
    return new TreeSet<Link>(LINK_COMPARATOR);
  }

  /** Create a new MemoryLinkStore sharing the same data structures as
   *  this one
   */
  public MemoryLinkStore newHandle() {
    return new MemoryLinkStore(linkdbs, nodedbs);
  }
  
  @Override
  public void initialize(Properties p, Phase currentPhase, int threadId)
      throws IOException, Exception {
  }

  @Override
  public void close() {
  }

  @Override
  public void clearErrors(int threadID) {
  }

  @Override
  public void addLink(String dbid, Link a, boolean noinverse) throws Exception {
    synchronized (linkdbs) {
      SortedSet<Link> links = findLinkByKey(dbid, a.id1, a.link_type, true);
  
      // Check for duplicates
      Iterator<Link> it = links.iterator();
      while (it.hasNext()) {
        Link existing = it.next();
        if (existing.id2 == a.id2) {
          it.remove();
        }
      }
      // Clone argument before inserting
      links.add(a.clone());
  
      /*System.err.println(String.format("added link (%d, %d, %d), %d in list",
                a.id1, a.link_type, a.id2, links.size()));*/
    }
  }

  @Override
  public void deleteLink(String dbid, long id1, long link_type, long id2,
      boolean noinverse, boolean expunge) throws Exception {
    synchronized (linkdbs) {
      //NOTE: does not reclaim space from unused structures
      SortedSet<Link> linkSet = findLinkByKey(dbid, id1, link_type, false);
      if (linkSet != null) {
        Iterator<Link> it = linkSet.iterator();
        while (it.hasNext()) {
          Link l = it.next();
          if (l.id2 == id2) {
            if (!expunge) {
              l.visibility = VISIBILITY_HIDDEN;
            } else {
              it.remove();
            }
            return;
          }
        }
      }
    }
  }

  @Override
  public void updateLink(String dbid, Link a, boolean noinverse)
      throws Exception {
    synchronized (linkdbs) {
      SortedSet<Link> linkSet = findLinkByKey(dbid, a.id1, a.link_type, false);
      if (linkSet != null) {
        Iterator<Link> it = linkSet.iterator();
        while (it.hasNext()) {
          Link l = it.next();
          if (l.id2 == a.id2) {
            it.remove();
            linkSet.add(a.clone());
            return;
          }
        }
      }
      
      // Throw error if updating non-existing link
      throw new Exception(String.format("Link not found: (%d, %d, %d)", a.id1,
                                                          a.link_type, a.id2));
    }
  }

  @Override
  public Link getLink(String dbid, long id1, long link_type, long id2)
      throws Exception {
    synchronized (linkdbs) {
      SortedSet<Link> linkSet = findLinkByKey(dbid, id1, link_type, false);
      if (linkSet != null) {
        for (Link l: linkSet) {
          if (l.id2 == id2) {
            return l.clone();
          }
        }
      }
      return null;
    }
  }

  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type)
      throws Exception {
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
  }

  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type,
      long minTimestamp, long maxTimestamp, int offset, int limit)
      throws Exception {
    int skipped = 0; // used for offset
    synchronized (linkdbs) {
      SortedSet<Link> linkSet = findLinkByKey(dbid, id1, link_type, false);
      if (linkSet == null || linkSet.size() == 0) {
        return null;
      } else {
        // Do a first pass to find size of result array
        int matching = 0;
        for (Link l: linkSet) {
          if (l.visibility == VISIBILITY_DEFAULT && 
                l.time >= minTimestamp && l.time <= maxTimestamp) {
            if (skipped < offset) {
              skipped++;
              continue;
            }
            matching++;
            if (matching >= limit) {
              break;
            }
          }
        }
        if (matching == 0) {
          return null;
        }
        Link res[] = new Link[matching];
        
        // Iterate in desc order of timestamp, break ties by id2
        int i = 0;
        skipped = 0;
        for (Link l: linkSet) {
          if (l.visibility == VISIBILITY_DEFAULT && 
                l.time >= minTimestamp && l.time <= maxTimestamp) {
            if (skipped < offset) {
              skipped++;
              continue;
            }
            res[i++] = l;
  
            if (i >= limit) {
              break;
            }
          }
        }
        return res;
      }
    }
  }

  @Override
  public long countLinks(String dbid, long id1, long link_type)
      throws Exception {
    synchronized(linkdbs) {
      SortedSet<Link> linkSet = findLinkByKey(dbid, id1, link_type, false);
      if (linkSet == null) {
        return 0;
      } else {
        // Count the number of visible links
        int visible = 0;
        for (Link l: linkSet) {
          if (l.visibility == VISIBILITY_DEFAULT) {
            visible++;
          }
        }
  
        /*System.err.println(
             String.format("Lookup (%d, %d): %d visible, %d total", id1, link_type,
                 visible, linkSet.size()));*/
        return visible;
      }
    }
  }

  /**
   * Should be called with lock on nodedbs held
   * @param dbid
   * @param autocreate
   * @return
   */
  private NodeDB getNodeDB(String dbid, boolean autocreate) throws Exception {
    NodeDB db = nodedbs.get(dbid);
    if (db == null) {
      if (autocreate) {
        db = new NodeDB();
        nodedbs.put(dbid, db);
      } else {
        /* Not initialized.. can't autocreate since we don't know the desired
         * start ID */
        throw new Exception("dbid " + dbid + " was not initialized");
      }
    }
    return db;
  }

  @Override
  public void resetNodeStore(String dbid, long startID) {
    synchronized(nodedbs) {
      NodeDB db = nodedbs.get(dbid);
      if (db == null) {
        nodedbs.put(dbid, new NodeDB(startID));
      } else {
        db.reset(startID);
      }
    }
  }

  @Override
  public long addNode(String dbid, Node node) throws Exception {
    synchronized(nodedbs) {
      NodeDB db = getNodeDB(dbid, false);
      long newId = db.allocateID();
      // Put copy of node in map
      Node inserted = node.clone();
      inserted.id = newId;
      Node prev = db.data.put(newId, inserted);
      if (prev != null) {
        throw new Exception("Internal error: node " + prev.toString()
            + " already existing in dbid " + dbid);
      }
      return newId;
    }
  }

  @Override
  public Node getNode(String dbid, int type, long id) throws Exception {
    synchronized(nodedbs) {
      NodeDB db = getNodeDB(dbid, false);
      Node n = db.data.get(id);
      if (n == null || n.type != type) {
        // Shouldn't return lookup on type mismatch
        return null;
      } else {
        return n.clone(); // return copy
      }
    }
  }

  @Override
  public boolean updateNode(String dbid, Node node) throws Exception {
    synchronized(nodedbs) {
      NodeDB db = getNodeDB(dbid, false);
      Node n = db.data.get(node.id);
      if (n == null || n.type != node.type) {
        // don't update on type mismatch
        return false;
      } else {
        // Store copy
        db.data.put(node.id, node.clone());
        return true;
      }
    }
  }

  @Override
  public boolean deleteNode(String dbid, int type, long id) throws Exception {
    synchronized(nodedbs) {
      NodeDB db = getNodeDB(dbid, false);
      Node n = db.data.get(id);
      if (n == null || n.type != type) {
        // don't delete on type mismatch
        return false;
      } else {
        db.data.remove(id);
        return true;
      }
    }
  }
}
