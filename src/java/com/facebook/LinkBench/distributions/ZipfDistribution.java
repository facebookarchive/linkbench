package com.facebook.LinkBench.distributions;

import java.util.Properties;
import java.util.Random;

import org.apache.commons.math3.util.FastMath;

public class ZipfDistribution implements ProbabilityDistribution {
  private long min = 0;
  private long max = 1;
  private double shape = 0.0;
  
  // precomputed values
  private double alpha = 0.0;
  private double eta = 0.0;
  private double zetan = 0.0;
  private double point5theta = 0.0;
  
  @Override
  public void init(long min, long max, Properties props, String keyPrefix) {
    if (max <= min) {
      throw new IllegalArgumentException("max = " + max + " <= min = " + min +
          ": probability distribution cannot have zero or negative domain");
    }
    if (max - min > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Zipf distribution does not currently " +
      		"support distributions with id range > " + Integer.MAX_VALUE +
      		": range [" + min + ", " + max + ") is too large.");
    }
    
    this.min = min;
    this.max = max;
    String shapeS = props != null ? props.getProperty(
                                  keyPrefix + "shape") : null;
    if (shapeS == null ) {
      throw new IllegalArgumentException("ZipfDistribution must be provided " +
      		keyPrefix + "shape parameter");
    }
    shape = Double.valueOf(shapeS);
    if (shape <= 0.0 || shape >= 1) {
      throw new IllegalArgumentException("Shape parameter " + shape + " is "
          + "out of valid range (0.0, 1.0)");
          
    }
    // Precompute some values to speed up future method calls
    long n = max - min;
    alpha = 1 / (1 - shape);
    zetan = Harmonic.generalizedHarmonic(n, shape);
    eta = (1 - FastMath.pow(2.0 / n, 1 - shape)) /
          (1 - Harmonic.generalizedHarmonic(2, shape) / zetan);
    point5theta = FastMath.pow(0.5, shape);
  }

  @Override
  public double cdf(long id) {
    if (id < min) return 0.0;
    if (id >= max) return 1.0;
    return Harmonic.generalizedHarmonic(id + 1 - min, shape) / zetan;
  }

  /**
   * Algorithm from "Quickly Generating Billion-Record Synthetic Databases",
   * Gray et. al., 1994
   * 
   * Pick a value in range [min, max) according to zipf distribution,
   * with min being the most likely to be chosen
   * 
   * parts of formula are precomputed in init since they are expensive
   * to calculate and only depend on the distribution parameters
   */
  @Override
  public long choose(Random rng) {
    double u = rng.nextDouble();
    double uz = u * zetan;
    long n = max - min;
    if (uz < 1) return min;
    if (uz < 1 + point5theta) return min + 1;
    return min + (long) 
          (n * FastMath.pow(eta * u - eta + 1, alpha));
  }
}
