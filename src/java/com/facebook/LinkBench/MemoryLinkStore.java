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
 * Simple in-memory implementation of a Linkstore
 * Not efficient or optimized at all, just for testing purposes.
 * Also not thread-safe.
 */
public class MemoryLinkStore extends LinkStore {
  private static class LookupKey {
    final long id1;
    final long link_type;
    
    public LookupKey(long id1, long link_type) {
      super();
      this.id1 = id1;
      this.link_type = link_type;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof LookupKey)) {
        return false;
      }
      LookupKey other2 = (LookupKey)other; 
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
  
  
  /** Simple hashmap implementation of LinkStore with nested maps: 
   * dbid ->  (id1, assoc_type) -> timestamp -> link */
  private final Map<String, Map<LookupKey, SortedSet<Link>>> linkdbs =
                                      new HashMap<String, Map<LookupKey, SortedSet<Link>>>();

  private static final Comparator<Link> LINK_COMPARATOR = new LinkTimeStampComparator();
  
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
    Map<LookupKey, SortedSet<Link>> db = linkdbs.get(dbid);
    if (db == null) {
      // Autocreate db
      db = new HashMap<LookupKey, SortedSet<Link>>();
      linkdbs.put(dbid, db);
    }
    LookupKey key = new LookupKey(id1, link_type);
    SortedSet<Link> links = db.get(key);
    if (links == null) {
      links = newSortedLinkSet();
      db.put(key, links);
    }
    return links;
  }

  private TreeSet<Link> newSortedLinkSet() {
    return new TreeSet<Link>(LINK_COMPARATOR);
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
    SortedSet<Link> links = findLinkByKey(dbid, a.id1, a.link_type, true);

    // Check for duplicates
    for (Link existing: links) {
      if (existing.id2 == a.id2) {
        throw new Exception(String.format("Link with key (%d, %d, %d) already"
            + "exists", a.id1, a.link_type, a.id2));
      }
    }
    // Clone argument before inserting
    links.add(a.clone());

    /*System.err.println(String.format("added link (%d, %d, %d), %d in list",
              a.id1, a.link_type, a.id2, links.size()));*/
  }

  @Override
  public void deleteLink(String dbid, long id1, long link_type, long id2,
      boolean noinverse, boolean expunge) throws Exception {
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
    throw new Exception(String.format("Link with key (%d, %d, %d) does not exist",
                                    id1, link_type, id2));
  }

  @Override
  public void updateLink(String dbid, Link a, boolean noinverse)
      throws Exception {
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
                                                                a.link_type));
  }

  @Override
  public Link getLink(String dbid, long id1, long link_type, long id2)
      throws Exception {
    SortedSet<Link> linkSet = findLinkByKey(dbid, id1, link_type, false);
    if (linkSet != null) {
      for (Link l: linkSet) {
        if (l.id2 == id2) {
          return l;
        }
      }
    }
    return null;
  }

  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type)
      throws Exception {
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, 10000);
  }

  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type,
      long minTimestamp, long maxTimestamp, int offset, int limit)
      throws Exception {
    SortedSet<Link> linkSet = findLinkByKey(dbid, id1, link_type, false);
    if (linkSet == null || linkSet.size() == 0) {
      return null;
    } else {
      // Do a first pass to find size of result array
      int matching = 0;
      for (Link l: linkSet) {
        if (l.visibility == VISIBILITY_DEFAULT && 
              l.time >= minTimestamp && l.time <= maxTimestamp) {
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
      for (Link l: linkSet) {
        if (l.visibility == VISIBILITY_DEFAULT && 
              l.time >= minTimestamp && l.time <= maxTimestamp) {
          res[i++] = l;

          if (i >= limit) {
            break;
          }
        }
      }
      return res;
    }
  }

  @Override
  public long countLinks(String dbid, long id1, long link_type)
      throws Exception {
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
