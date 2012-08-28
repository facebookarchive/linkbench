package com.facebook.LinkBench.distributions;

import java.util.Properties;
import java.util.Random;

/**
 * Uniform distribution over integers in range [minID, maxID),
 * where minID is included in range and maxID excluded
 *
 */
public class UniformDistribution implements ProbabilityDistribution {
  private long min = 0; 
  private long max = 1;
  
  public void init(long min, long max, Properties props, String keyPrefix) {
    if (max <= min) {
      throw new IllegalArgumentException("max = " + max + " <= min = " + min +
          ": probability distribution cannot have zero or negative domain");
    }
    this.min = min;
    this.max = max;
  }
  
  /**
   * Cumulative distribution function for distribution
   * @param id
   * @return
   */
  public double cdf(long id) {
    if (id >= max) {
      return 1.0;
    }
    if (id < min) {
      return 0.0;
    }
    long n = max - min;
    long rank = id - min + 1;
    
    return rank / (double)n;
  }
  
  /**
   * Quantile function
   */
  public long quantile(double p) {
    assert(p >= 0.0 && p <= 1.0);
    long n = max - min;
    long i = Math.round(p * n);
    return i + min;
  }
  
  /** Choose an id X uniformly in the range*/
  public long choose(Random rng) {
    long n = max - min;
    // Java's random number generator has less randomness in lower bits
    // so just taking a mod doesn't give a good quality result.
    if (n <= Integer.MAX_VALUE) {
      return min + (long)rng.nextInt((int)n);
    } else {
      long i = rng.nextInt();
      long j = rng.nextInt((int)(n / Integer.MAX_VALUE));
      return (Integer.MAX_VALUE * (long)j) + i;
    }
  }
}
