/*
 * Copyright 2012, Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
