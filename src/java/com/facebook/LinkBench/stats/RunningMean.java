package com.facebook.LinkBench.stats;

/** 
 * Keep track of mean in numerically stable way
 * See "Comparison of Several Algorithms for Computing Sample Means and 
 *      Variances", Ling, 1974, J. American Stat. Assoc.
 */
public class RunningMean {
  /** Number of samples */
  private long n;

  /** First sample */
  private final double v1;
  
  /** sum of difference */
  private double running;
  
  /** initialize with first sample */
  public RunningMean(double v1) {
    super();
    this.v1 = v1;
    this.n = 1;
    this.running = 0.0;
  }

  public void addSample(double vi) {
    n++;
    running += (vi - v1);
  }
  
  public double mean() {
    return v1 + running / n;
  }

  public long samples() {
    return n;
  }
}