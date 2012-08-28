package com.facebook.LinkBench;

import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase;

import org.junit.Test;

import com.facebook.LinkBench.distributions.ProbabilityDistribution;

/**
 * This test implements generic unit tests for different implementations of
 * ProbabilityDistribution. 
 * 
 * Most of these tests are either sanity tests (check that output is within
 * expected range and obeys basic invariants), and consistency tests
 * (check that the output of two different methods is consistent).
 * 
 * While these tests go a long way to checking the consistency of the
 * behavior of the ProbabilityDistribution, it cannot check that the
 * specific correct values are generated: it is helpful to implement
 * additional tests for each concrete implementation.
 * 
 * @author tarmstrong
 */
public abstract class DistributionTestBase extends TestCase {

  protected abstract ProbabilityDistribution getDist();
  
  protected Properties getDistParams() {
    return new Properties();
  }
  
  /** Number of cdf checks to perform */
  protected int cdfChecks() {
    return 50000;
  }

  protected Bucketer getBucketer() {
    return new UniformBucketer(cdfChecks());
  }

  /** Percentage difference between cdf and choose() to tolerate */ 
  protected double tolerance() {
    return 0.002;
  }

  public Random initRandom(String testName) {
    long seed = System.currentTimeMillis();
    System.err.println("Choose seed " + seed + " for test " + testName);
    return new Random(seed);
  }
  
  /**
   * Check a few invariants cdf should adhere to
   */
  @Test
  public void testCDFSanity() {
    ProbabilityDistribution dist = getDist();
    long min = 453, max = 26546454;
    dist.init(min, max, getDistParams(), "");
    
    assertEquals(dist.cdf(min-1), 0.0);
    assertEquals(dist.cdf(min-234321), 0.0);
    assertEquals(dist.cdf(max), 1.0);
    assertEquals(dist.cdf(max+2343242224234L), 1.0);
    
    // Check cdf is monotonically increasing
    double last = 0.0;
    long step = (max - min) / cdfChecks();
    for (long id = min; id < max; id += step) {
      double p = dist.cdf(id);
      assertTrue(p >= last);
      last = p;
    }
  }

  @Test
  public void testChooseSanity() {
    ProbabilityDistribution dist = getDist();
    long min = 453, max = 26546454;
    dist.init(min, max, getDistParams(), "");
    Random rng = initRandom("testChooseSanity");
    for (int i = 0; i < 100000; i++) {
      long id = dist.choose(rng);
      assertTrue(id >= min);
      assertTrue(id < max);
    }
  }
  
  /**
   * Check that choose() and cdf() are returning consistent results
   * (i.e. that the result of choose are distributed according to cdf)
   */
  @Test
  public void testCDFChooseConsistency() {
    Bucketer bucketer = getBucketer();
    int bucketCount = bucketer.getBucketCount();
    int buckets[] = new int[bucketCount];
    long min = 252352, max = 6544543;
    long n = max - min;
    
    Random rng = initRandom("testCDFChooseConsistency");
    ProbabilityDistribution dist = getDist();
    dist.init(min, max, getDistParams(), "");
    
    int trials = 1000000;
    for (int i = 0; i < trials; i++) {
      long id = dist.choose(rng);
      long off = id - min;
      int bucket = bucketer.chooseBucket(off, n);
      buckets[bucket]++;
    }
    
    int totalCount = 0;
    boolean fail = false;
    for (int b = 0; b < bucketCount; b++) {
      totalCount += buckets[b];
      long bucketTop = bucketer.bucketMax(b, n) + min;
      double actCDF = ((double)totalCount) / trials;
      double expCDF = dist.cdf(bucketTop);
      // 0.2% error
      if (Math.abs(expCDF - actCDF) > tolerance()) {
        System.err.println(String.format("Divergence between CDF and " +
        		"choose function: P(X <= %d) act: %f exp: %f", bucketTop,
        		actCDF, expCDF));
        fail = true;
      }
    }
    if (fail) {
      fail("Divergence between cdf and choose methods: see preceding output " +
      		"for details");
    }
  }

  /**
   * Different distributions should be bucketed in different ways
   * to test their fit.  For example, the zipf distribution treats
   * lower keys specially so we want to have better resolution for 
   * those
   */
  static interface Bucketer {
    public int getBucketCount();
    public int chooseBucket(long i, long n);
    public long bucketMax(int bucket, long n);
  }
  
  static class UniformBucketer implements Bucketer {
    final int bucketCount;
    
    public UniformBucketer(int bucketCount) {
      this.bucketCount = bucketCount;
    }

    public int getBucketCount() {
      return bucketCount;
    }
    
    public int chooseBucket(long i, long n) {
      return (int)((i * bucketCount) / n);
    }
    
    public long bucketMax(int bucket, long n) {
      return ((long)((((double)bucket+1)/bucketCount)*n))  - 1; 
    }
  }
}
