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

import java.util.Properties;

import com.facebook.LinkBench.distributions.ProbabilityDistribution;
import com.facebook.LinkBench.distributions.ZipfDistribution;

public class ZipfDistTest extends DistributionTestBase {

  @Override
  protected int cdfChecks() {
    // Don't do many checks, since its expensive
    return 5;
  }

  @Override
  public ProbabilityDistribution getDist() {
    return new ZipfDistribution();
  }

  @Override
  public Properties getDistParams() {
    Properties props = new Properties();
    props.setProperty("shape", "0.9");
    return props;
  }


  @Override
  protected Bucketer getBucketer() {
    return new ZipfBucketer();
  }

  @Override
  protected double tolerance() {
    /*
     * Method for choosing IDs isn't 100% precise
     * but something is more seriously wrong if it is more than 1% off
     */
    return 0.01;
  }

  /**
   * Check distribution more closely at low values
   */
  static class ZipfBucketer implements Bucketer {
    private static final long bucketBounds[] =
      {0, 1, 2, 3, 4, 10, 20, 30, 100, 1000, 10000, 100000, 1000000,
        10000000, 100000000};

    public int getBucketCount() {
      return bucketBounds.length + 1;
    }

    public int chooseBucket(long i, long n) {
      for (int j = 0; j < bucketBounds.length; j++) {
        if (i <= bucketBounds[j]) {
          return j;
        }
      }
      return bucketBounds.length;
    }

    public long bucketMax(int bucket, long n) {
      if (bucket >= bucketBounds.length) return n;
      else return bucketBounds[bucket];
    }
  }
}
