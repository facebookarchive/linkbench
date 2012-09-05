package com.facebook.LinkBench;

import java.util.Properties;

import com.facebook.LinkBench.distributions.LogNormalDistribution;
import com.facebook.LinkBench.distributions.ProbabilityDistribution;

public class LogNormalTest extends DistributionTestBase {

  @Override
  protected ProbabilityDistribution getDist() {
    return new LogNormalDistribution();
  }

  @Override
  protected Properties getDistParams() {
    Properties props = new Properties();
    props.setProperty(LogNormalDistribution.CONFIG_SIGMA, "1");
    props.setProperty(LogNormalDistribution.CONFIG_MEDIAN, "5000");
    return props;
  }

  @Override
  protected double tolerance() {
    return 0.05;
  }
  
  /**
   * Sanity check values
   */
  public void testLogNormal() {
    LogNormalDistribution d = new LogNormalDistribution();
    int median = 10;
    d.init(0, 100, median, 1);
    // CDF of median should be 0.5 by def.
    assertEquals(0.5, d.cdf(median), 0.01);
    
    
    // Precomputed points
    d.init(0, 1000, 100, 1);
    assertEquals(0.033434, d.cdf(16), 0.0001);
    assertEquals(0.327695, d.cdf(64), 0.0001);
    assertEquals(0.597491, d.cdf(128), 0.0001);
    assertEquals(0.94878, d.cdf(512), 0.0001);
  }

  @Override
  public void testPDFSanity() {
    System.err.println("test not implemented");
  }

  @Override
  public void testPDFSum() {
    System.err.println("test not implemented");
  }

  @Override
  public void testCDFPDFConsistency() {
    System.err.println("test not implemented");
  }

  @Override
  public void testQuantileSanity() {
    System.err.println("test not implemented");
  }

}
