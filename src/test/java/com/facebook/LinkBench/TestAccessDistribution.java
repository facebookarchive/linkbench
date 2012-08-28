package com.facebook.LinkBench;

import java.io.File;
import java.util.Properties;
import java.util.Random;

import org.junit.Test;

import com.facebook.LinkBench.RealDistribution.DistributionType;
import com.facebook.LinkBench.distributions.AccessDistributions.AccessDistMode;
import com.facebook.LinkBench.distributions.AccessDistributions.AccessDistribution;
import com.facebook.LinkBench.distributions.AccessDistributions.BuiltinAccessDistribution;
import com.facebook.LinkBench.distributions.AccessDistributions.ProbAccessDistribution;
import com.facebook.LinkBench.distributions.UniformDistribution;
import com.facebook.LinkBench.distributions.ZipfDistribution;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

public class TestAccessDistribution extends TestCase {

  @Test
  public void testMultiple() {
    testSanityBuiltinDist(AccessDistMode.MULTIPLE, 3);
  }

  @Test
  public void testPerfectPower() {
    testSanityBuiltinDist(AccessDistMode.PERFECT_POWER, 3);
  }

  @Test
  public void testPower() {
    testSanityBuiltinDist(AccessDistMode.POWER, 3);
  }

  @Test
  public void testReciprocal() {
    testSanityBuiltinDist(AccessDistMode.RECIPROCAL, 3);
  }

  @Test
  public void testRoundRobin() {
    testSanityBuiltinDist(AccessDistMode.ROUND_ROBIN, 0);
  }
  
  @Test
  public void testUniform() {
    UniformDistribution u = new UniformDistribution();
    Properties props = new Properties();
    int min = 100, max = 200;
    u.init(min, max, props, "");
    ProbAccessDistribution unshuffled = new ProbAccessDistribution(u, null);
    testSanityAccessDist(unshuffled, min, max);
    
    ProbAccessDistribution shuffled = new ProbAccessDistribution(u, 
                        new InvertibleShuffler(13, 25, max - min));
    testSanityAccessDist(shuffled, min, max);
  }

  @Test
  public void testZipf() {
    ZipfDistribution z = new ZipfDistribution();
    Properties props = new Properties();
    props.setProperty("shape", "0.5");
    int min = 100, max = 200;
    z.init(min, max, props, "");
    ProbAccessDistribution unshuffled = new ProbAccessDistribution(z, null);
    testSanityAccessDist(unshuffled, min, max);
    
    ProbAccessDistribution shuffled = new ProbAccessDistribution(z, 
                        new InvertibleShuffler(13, 25, max - min));
    testSanityAccessDist(shuffled, min, max);
  }
  
  
  @Test
  public void testReal() {
    RealDistribution r = new RealDistribution();
    Properties props = new Properties();
    props.setProperty(Config.DISTRIBUTION_DATA_FILE, 
        new File("config/Distribution.dat").getAbsolutePath());
    int min = 100, max = 200;
    r.init(props, min, max, DistributionType.READS);
    ProbAccessDistribution unshuffled = new ProbAccessDistribution(r, null);
    testSanityAccessDist(unshuffled, min, max);
    
    ProbAccessDistribution shuffled = new ProbAccessDistribution(r, 
                        new InvertibleShuffler(13, 25, max - min));
    testSanityAccessDist(shuffled, min, max);
  }
  
  

  public static void testSanityBuiltinDist(AccessDistMode mode, long config) {
    long minid = 123;
    long maxid = 12345;
    BuiltinAccessDistribution dist = new BuiltinAccessDistribution(mode, minid,
        maxid, config);
    testSanityAccessDist(dist, minid, maxid);
  }

  /**
   * Check that results are in range, etc.
   */
  public static void testSanityAccessDist(AccessDistribution dist, long minid,
      long maxid) {
    long seed = System.currentTimeMillis();
    System.err.println("Using seed " + seed);
    Random rng = new Random(seed);
    long id = 1;
    int trials = 10000;
    long start = System.currentTimeMillis();
    for (int i = 0; i < trials; i++) {
      id = dist.nextID(rng, id);
      try {
        assertTrue(id >= minid);
        assertTrue(id < maxid);
      } catch (AssertionFailedError e) {
        System.err.println("Error: on trial " + i + " id returned: " 
            + id + " not in range [" + minid + "," + maxid + ")");
        throw e;
      }
    }
    long end = System.currentTimeMillis();
    System.err.println("Took " + (end - start) + " ms for " + trials
                                                            + " trials");
  }

}
