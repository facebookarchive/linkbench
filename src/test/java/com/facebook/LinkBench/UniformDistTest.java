package com.facebook.LinkBench;

import com.facebook.LinkBench.distributions.ProbabilityDistribution;
import com.facebook.LinkBench.distributions.UniformDistribution;

public class UniformDistTest extends DistributionTestBase {

  @Override
  public ProbabilityDistribution getDist() {
    return new UniformDistribution();
  }

}
