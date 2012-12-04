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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase;

import org.junit.Test;

import com.facebook.LinkBench.distributions.ID2Chooser;
import com.facebook.LinkBench.distributions.ZipfDistribution;

public class ID2ChooserTest extends TestCase {
  Properties props;
  
  @Override
  public void setUp() {
    props = new Properties();
    props.setProperty(Config.RANDOM_ID2_MAX, "0");
    props.setProperty(Config.NLINKS_FUNC, ZipfDistribution.class.getName());
    props.setProperty(Config.NLINKS_PREFIX + "shape", "0.5");
    props.setProperty(Config.NLINKS_PREFIX + "scale", "1000000");
  }
  
  @Test
  public void testNoLoadCollisions() {
    long n = 10000;
    long min = 500;
    long max = n + min;
    long seed = 5313242;
    Random rng = new Random(seed);
    
    ID2Chooser c = new ID2Chooser(props, min, max, 1, 1);
    // Check we don't get the same id2 more than once (i.e. duplicate links)
    
    int nlinks = 50;
    HashMap<Long, Integer> seen = new HashMap<Long, Integer>();
    
    long id1 = 1234;
    for (int i = 0; i < nlinks; i++) {
      long id2 = c.chooseForLoad(rng, id1, LinkStore.DEFAULT_LINK_TYPE, i);
      Integer j = seen.get(id2);
      if (j != null) {
        fail("Same link generated twice: (" + id1 + ", " + id2 + ") for " +
        		" indices " + j + " and " + i);
      }
      seen.put(id2, i);
    }
  }
  
  @Test
  public void testChooseForOp() {
    // Currently just exercise the code: I don't have a good way to verify the
    // output is as intended
    long seed = 1643;
    Random rng = new Random(seed);
    long n = 10000;
    long min = 500;
    long max = n + min;
    int trials = 1000;
    ID2Chooser c = new ID2Chooser(props, min, max, 1, 1);
    for (int i = 0; i < trials; i++) {
      long id2 = c.chooseForOp(rng, i + min, LinkStore.DEFAULT_LINK_TYPE, 1.0);
      assert(id2 >= min);
      
      id2 = c.chooseForOp(rng, i + min, LinkStore.DEFAULT_LINK_TYPE, 0.5);
      assert(id2 >= min);
    }
  }
  
  /**
   * Check that the choosing mechanism is generating id2s with the
   * right probability of a loaded link matching
   */
  @Test
  public void testMatchPercent() {
    long seed = 15325435L;
    Random rng = new Random(seed);
    
    int minid = 500, maxid=1000000;
    ID2Chooser chooser = new ID2Chooser(props, minid, maxid, 1, 0);
    for (int id1 = minid; id1 < maxid; id1 += 3763) {
      HashSet<Long> existing = new HashSet<Long>();
      long nlinks = chooser.calcTotalLinkCount(id1);
      for (long i = 0; i < nlinks; i++) {
        long id2 = chooser.chooseForLoad(rng, id1,
                                LinkStore.DEFAULT_LINK_TYPE, i);
        existing.add(id2);
      }
      
      int trials = 10000;
      
      int hit = 0; // hit for prob = 50%
      
      for (int i = 0; i < trials; i++) {
        // Test with 100% prob of hit
        long id2 = chooser.chooseForOp(rng, id1, 
                                          LinkStore.DEFAULT_LINK_TYPE, 1.0);
        assertTrue(existing.contains(id2) || existing.size() == 0);
        
        // Test with 50% prob of hit
        id2 = chooser.chooseForOp(rng, id1, LinkStore.DEFAULT_LINK_TYPE, 0.5);
        if (existing.contains(id2)) {
          hit++;
        }
      }
      
      double hitPercent = hit / (double)trials;
      if (existing.size() > 0 && Math.abs(0.5 - hitPercent) > 0.05) {
        fail(hitPercent * 100 + "% of ids2 were hits for id1 " + id1);
      }
    }
  }
  
  /**
   * Check that link counts work for multiple link types
   */
  @Test
  public void testLinkCount() {
    long startid = 1, maxid = 1000;
    Properties newProps = new Properties(props);
    int nLinkTypes = 10;
    newProps.setProperty(Config.LINK_TYPE_COUNT, Integer.toString(nLinkTypes));
    
    ID2Chooser chooser = new ID2Chooser(newProps, startid, maxid, 1, 0);
    long linkTypes[] = chooser.getLinkTypes();
    assertEquals(nLinkTypes, linkTypes.length);
    
    // Check it works for some different IDs
    for (long id = startid; id < maxid; id += 7) {
      long totalCount = 0;
      for (long linkType: linkTypes) {
        totalCount += chooser.calcLinkCount(id, linkType); 
      }
      assertEquals(chooser.calcTotalLinkCount(id), totalCount);
    }
  }
}
