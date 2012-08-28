package com.facebook.LinkBench;

import java.util.ArrayList;
import java.util.Properties;

import com.facebook.LinkBench.distributions.PiecewiseLinearDistribution;
import com.facebook.LinkBench.distributions.PiecewiseLinearDistribution.Point;
import com.facebook.LinkBench.distributions.ProbabilityDistribution;

public class PiecewiseDistTest extends DistributionTestBase {

  ArrayList<Point> testDistribution = null;
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // Make up an arbitrary distribution
    testDistribution = new ArrayList<Point>();
    testDistribution.add(new Point(0, 0.1));
    testDistribution.add(new Point(1, 0.15));
    testDistribution.add(new Point(2, 0.17));
    testDistribution.add(new Point(3, 0.20));
    testDistribution.add(new Point(4, 0.23));
    testDistribution.add(new Point(10, 0.26));
    testDistribution.add(new Point(20, 0.4));
    testDistribution.add(new Point(30, 0.45));
    testDistribution.add(new Point(40, 0.6));
    testDistribution.add(new Point(55, 0.64));
    testDistribution.add(new Point(70, 0.70));
    testDistribution.add(new Point(90, 0.75));
    testDistribution.add(new Point(100, 0.82));
    testDistribution.add(new Point(110, 0.92));
    testDistribution.add(new Point(120, 1.0));
  }
  
  @Override
  protected int cdfChecks() {
    return 50;
  }

  @Override
  protected ProbabilityDistribution getDist() {
    return new PiecewiseLinearDistribution() {

      @Override
      public void init(long min, long max, Properties props, String keyPrefix) {
        init(min, max, testDistribution);
      }
    };
  }
  
  @Override
  public void testCDFSanity() {
    System.err.println("CDF not implemented");
  }
  
  @Override
  public void testCDFChooseConsistency() {
    System.err.println("CDF not implemented");
  }
  
  @Override
  public void testCDFPDFConsistency() {
    System.err.println("CDF not implemented");
  }
  
  @Override
  public void testQuantileSanity() {
    System.err.println("Quantile not implemented");
  }
  
}
