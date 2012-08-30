package com.facebook.LinkBench.distributions;

import java.util.Properties;
import java.util.Random;

import org.apache.commons.math3.util.FastMath;

import com.facebook.LinkBench.Config;

/**
 * Geometric distribution
 * 
 * NOTE: this generates values in the range [min, max).  Since the
 * real geometric distribution generates values in range [min, inf),
 * we truncate anything >= max
 */
public class GeometricDistribution implements ProbabilityDistribution {

  /** The probability parameter that defines the distribution */
  private double p = 0.0;
  
  /** Valid range */
  private long min = 0, max = 0;
  
  private double scale = 0.0;
  
  public static final String PROB_PARAM_KEY = "prob";
  
  @Override
  public void init(long min, long max, Properties props, String keyPrefix) {
    
    String pStr = props.getProperty(keyPrefix + PROB_PARAM_KEY);
    if (pStr == null) {
      throw new IllegalArgumentException("Expected properties key " +
                                         keyPrefix + PROB_PARAM_KEY);
    }
    double parsedP = Double.parseDouble(pStr);
    
    String scaleStr = props.getProperty(keyPrefix + 
                                  Config.PROB_SCALE);
    double scaleVal = 1.0;
    if (scaleStr != null) {
      scaleVal = Double.parseDouble(scaleStr);
    }
    init(min, max, parsedP, scaleVal);
  }

  public void init(long min, long max, double p, double scale) {
    this.min = min;
    this.max = max;
    this.p = p;
    this.scale = scale;
  }

  @Override
  public double pdf(long id) {
    return scaledPdf(id, 1.0);
  }

  @Override
  public double expectedCount(long id) {
    return scaledPdf(id, scale);
  }
  
  private double scaledPdf(long id, double scaleFactor) {
    if (id < min || id >= max) return 0.0;
    long x = id - min;
    return FastMath.pow(1 - p, x) * scaleFactor * p;
  }

  @Override
  public double cdf(long id) {
    if (id < min) return 0.0;
    if (id >= max) return 1.0;
    return 1 - FastMath.pow(1 - p, id - min + 1);
  }

  @Override
  public long choose(Random rng) {
    return quantile(rng.nextDouble());
  }

  @Override
  public long quantile(double r) {
    /*
     * Quantile function for geometric distribution over
     * range [0, inf) where 0 < r < 1
     * quantile(r) = ceiling(ln(1 - r) / ln (1 - p))
     * Source: http://www.math.uah.edu/stat/bernoulli/Geometric.html
     */
    if (r == 0.0) return min; // 0.0 must be handled specially
    
    long x = min + (long)FastMath.ceil(
            FastMath.log(1 - r) / FastMath.log(1 - p));
    // truncate over max
    return Math.min(x, max - 1); 
  }

}
