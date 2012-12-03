package com.facebook.LinkBench;

import java.util.Random;

import org.junit.Test;

import com.facebook.LinkBench.distributions.ProbabilityDistribution;
import com.facebook.LinkBench.distributions.UniformDistribution;

public class UniformDistTest extends DistributionTestBase {

  @Override
  public ProbabilityDistribution getDist() {
    return new UniformDistribution();
  }
  
  @Test
  public void testInRange() {
    // Check 2^31 < n < 2^32 and n > 2^32
    long maxes[] = {(long)Math.pow(2, 31.5), (long)Math.pow(2, 34.23)};
    int trials = 10000;
    Random rng = new Random();
    
    for (long max: maxes) {
      UniformDistribution dist = new UniformDistribution();
      dist.init(0, max, 1);
      for (int trial = 0; trial < trials; trial++) {
        long i = dist.choose(rng);
        assertTrue(i >= 0);
        assertTrue(i < max);
        System.err.println(i);
      }
    }
  }
}
