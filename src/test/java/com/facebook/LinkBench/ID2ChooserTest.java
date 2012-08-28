package com.facebook.LinkBench;

import java.util.HashMap;
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
      long id2 = c.chooseForLoad(rng, id1, i);
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
      long id2 = c.chooseForOp(rng, i + min, 1.0);
      assert(id2 >= min);
      
      id2 = c.chooseForOp(rng, i + min, 0.5);
      assert(id2 >= min);
    }
  }
}
