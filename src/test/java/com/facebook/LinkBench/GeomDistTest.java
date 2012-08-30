package com.facebook.LinkBench;

import java.util.Properties;

import org.junit.Test;

import com.facebook.LinkBench.distributions.GeometricDistribution;
import com.facebook.LinkBench.distributions.ProbabilityDistribution;

public class GeomDistTest extends DistributionTestBase {

  @Override
  protected ProbabilityDistribution getDist() {
    return new GeometricDistribution();
  }

  @Override
  protected Properties getDistParams() {
    Properties props = new Properties();
    props.setProperty(GeometricDistribution.PROB_PARAM_KEY, "0.2");
    return props;
  }

  /**
   * Test cdf and pdf against precalculated values
   */
  @Test
  public void testGeom() {
    GeometricDistribution d = new GeometricDistribution();
    d.init(1, Long.MAX_VALUE, 0.3, 1.0);
    assertEquals(0.3, d.cdf(1), 0.001);
    assertEquals(0.51, d.cdf(2), 0.001);
    assertEquals(0.657, d.cdf(3), 0.001);
    assertEquals(0.917646, d.cdf(7), 0.001);
    assertEquals(0.971752, d.cdf(10), 0.001);
    assertEquals(0.995252, d.cdf(15), 0.001);
    
    assertEquals(0.3, d.pdf(1), 0.001);
    assertEquals(0.21, d.pdf(2), 0.001);
    assertEquals(0.147, d.pdf(3), 0.001);
    assertEquals(0.035, d.pdf(7), 0.001);
    assertEquals(0.012106, d.pdf(10), 0.001);
    assertEquals(0.002035, d.pdf(15), 0.001);
  }
}
