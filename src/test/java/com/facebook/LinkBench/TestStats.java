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

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import org.junit.Test;

import com.facebook.LinkBench.stats.LatencyStats;

public class TestStats extends TestCase {

  @Test
  public void testBucketing() {
    // 0 microseconds until 100 seconds
    for (long us = 0; us < 100 * 1000 * 1000; us += 100 ) {
      int bucket = LatencyStats.latencyToBucket(us);
      try {
        assertTrue(bucket >= 0);
        assertTrue(bucket < LatencyStats.NUM_BUCKETS);
        long range[] = LatencyStats.bucketBound(bucket);
        assertTrue(us >= range[0]);
        assertTrue(us < range[1]);
      } catch (AssertionFailedError e) {
        System.err.println("Failed for " + us + "us, bucket=" + bucket);
        throw e;
      }
    }
  }
}
