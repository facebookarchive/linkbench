package com.facebook.LinkBench.distributions;

import java.util.Properties;
import java.util.Random;

import org.apache.commons.math3.util.FastMath;

import com.facebook.LinkBench.ConfigUtil;

public class LogNormalDistribution implements ProbabilityDistribution {
  private long min;
  private long max;
  private double mu; // mean of the natural log of random variable
  private double sigma; // standard deviation of natural log of random variable
  
  public static final String CONFIG_MEDIAN = "median";
  public static final String CONFIG_SIGMA = "sigma";
  
  @Override
  public void init(long min, long max, Properties props, String keyPrefix) {
    double sigma = ConfigUtil.getDouble(props, CONFIG_SIGMA);
    double median = ConfigUtil.getDouble(props, CONFIG_MEDIAN);
    init(min, max, median, sigma);
  }
  
  /**
   * 
   * @param min
   * @param max
   * @param median the median value of the distribution
   * @param sigma the standard deviation of the natural log of the variable
   * @param scale
   */
  public void init(long min, long max, double median, double sigma) {
    this.min = min;
    this.max = max;
    this.mu = FastMath.log(median);
    this.sigma = sigma;
  }

  @Override
  public double pdf(long id) {
    throw new RuntimeException("pdf not implemented");
  }

  @Override
  public double expectedCount(long id) {
    throw new RuntimeException("expectedCount not implemented");
  }

  @Override
  public double cdf(long id) {
    if (id < min) return 0.0;
    if (id >= max) return 1.0;
    org.apache.commons.math3.distribution.LogNormalDistribution d = 
        new org.apache.commons.math3.distribution.LogNormalDistribution(mu, sigma);
    return d.cumulativeProbability(id);
  }

  @Override
  public long choose(Random rng) {
    long choice = (long) Math.round(FastMath.exp((rng.nextGaussian() * sigma) + mu));
    if (choice < min) 
      return min;
    else if (choice >= max) 
      return max - 1;
    else 
      return choice;
  }

  @Override
  public long quantile(double p) {
    throw new RuntimeException("Quantile not implemented");
  }

}
