package com.facebook.LinkBench.distributions;

import java.util.Properties;
import java.util.Random;

import com.facebook.LinkBench.Config;
import com.facebook.LinkBench.ConfigUtil;
import com.facebook.LinkBench.InvertibleShuffler;
import com.facebook.LinkBench.LinkStore;
import com.facebook.LinkBench.RealDistribution;
import com.facebook.LinkBench.RealDistribution.DistributionType;
import com.facebook.LinkBench.distributions.LinkDistributions.LinkDistribution;

/**
 * Encapsulate logic for choosing id2s for request workload.
 * @author tarmstrong
 *
 */
public class ID2Chooser {

  private final long startid1;
  private long maxid1;

  /** if > 0, choose id2s in range [startid1, randomid2max) */
  private final long randomid2max;
  
  /** Number of distinct link types */
  private final int linkTypeCount;
  
  private final InvertibleShuffler nLinksShuffler;
  // #links distribution from properties file
  private final LinkDistribution linkDist;
  
  // configuration for generating id2
  private final int id2gen_config;
  
  // Information about number of request threads, used to generate
  // thread-unique id2s
  private final int nrequesters;
  private final int requesterID;

  public ID2Chooser(Properties props, long startid1, long maxid1,
                    int nrequesters, int requesterID) {
    this.startid1 = startid1;
    this.maxid1 = maxid1;
    this.nrequesters = nrequesters;
    this.requesterID = requesterID;
    
    // random number generator for id2
    randomid2max = ConfigUtil.getLong(props, Config.RANDOM_ID2_MAX);
    
    // configuration for generating id2
    id2gen_config = ConfigUtil.getInt(props, Config.ID2GEN_CONFIG, 0);
    
    linkTypeCount = ConfigUtil.getInt(props, Config.LINK_TYPE_COUNT, 1);
    linkDist = LinkDistributions.loadLinkDistribution(props, startid1, maxid1);
    nLinksShuffler = RealDistribution.getShuffler(DistributionType.LINKS,
                                                 maxid1 - startid1);
  }
  

  /**
   * Choose an ids
   * @param rng
   * @param id1
   * @param link_type
   * @param outlink_ix this is the ith link of this type for this id1 
   * @return
   */
  public long chooseForLoad(Random rng, long id1, long link_type, 
                                                  long outlink_ix) {
    if (randomid2max == 0) {
      return id1 + outlink_ix;
    } else {
      return rng.nextInt((int)randomid2max);
    }
  }
  
  /**
   * Choose an id2 for an operation given an id1 
   * @param id1
   * @param linkType
   * @param pExisting approximate probability that id should be in 
   *        existing range
   * @return
   */
  public long chooseForOp(Random rng, long id1, long linkType, 
                                                double pExisting) {
    long nlinks = calcLinkCount(id1, linkType);
  
    // We want to sometimes add a link that already exists and sometimes
    // add a new link. So generate id2 such that it has roughly pExisting 
    // chance of already existing.
    // This happens unless randomid2max is non-zero (in which case just pick a 
    // random id2 upto randomid2max). 
    long range = (long) Math.round((1/pExisting) * nlinks);
    range = Math.max(range, 1); // Ensure non-empty range
    long id2; 
    if (randomid2max == 0) {
      id2 = id1 + rng.nextInt((int)range);
    } else {
      id2 = rng.nextInt((int)randomid2max);
    }
  
    if (id2gen_config == 1) {
      return fixId2(id2, nrequesters, requesterID, randomid2max);
    } else {
      return id2;
    }
  }

  public boolean sameShuffle;
  /** 
   * Calculates the original number of outlinks for a given id1 (i.e. the
   * number that would have been loaded)
   * Sets sameShuffle field to true if shuffled was same as original
   * @return number of links for this id1
   */
  public long calcTotalLinkCount(long id1) {
    assert(id1 >= startid1 && id1 < maxid1);
    // Shuffle.  A low id after shuffling means many links, a high means few
    long shuffled;
    if (linkDist.doShuffle()) { 
      shuffled = startid1 + nLinksShuffler.invertPermute(id1 - startid1);
    } else {
      shuffled = id1;
    }
    assert(shuffled >= startid1 && shuffled < maxid1);
    sameShuffle = shuffled == id1;
    long nlinks = linkDist.getNlinks(shuffled);
    return nlinks;
  }

  // return a new id2 that satisfies 3 conditions:
  // 1. close to current id2 (can be slightly smaller, equal, or larger);
  // 2. new_id2 % nrequesters = requestersId;
  // 3. smaller or equal to randomid2max unless randomid2max = 0
  private static long fixId2(long id2, long nrequesters,
                             long requesterID, long randomid2max) {

    long newid2 = id2 - (id2 % nrequesters) + requesterID;
    if ((newid2 > randomid2max) && (randomid2max > 0)) newid2 -= nrequesters;
    return newid2;
  }
  
  public long[] getLinkTypes() {
    long res[] = new long[linkTypeCount];
    // Just have link types in a sequence starting at the default one
    for (int i = 0; i < linkTypeCount; i++) {
      res[i] = LinkStore.DEFAULT_LINK_TYPE + i;
    }
    return res;
  }
  
  /**
   * Choose a link type.
   * For now just select each type with equal probability.
   */
  public long chooseRandomLinkType(Random rng) {
    return LinkStore.DEFAULT_LINK_TYPE + rng.nextInt(linkTypeCount);
  }
  
  public long calcLinkCount(long id1, long linkType) {
    // Divide total links between types so that total is correct 
    long totCount = calcTotalLinkCount(id1);
    long minCount = totCount / linkTypeCount;
    long leftOver = totCount - minCount;
    int typeNum = (int)(linkType -LinkStore.DEFAULT_LINK_TYPE);
    if (typeNum < leftOver) {
      return minCount + 1;
    } else {
      return minCount;
    }
  }

}
