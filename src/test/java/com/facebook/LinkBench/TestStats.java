package com.facebook.LinkBench;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import org.junit.Test;

public class TestStats extends TestCase {
  
  @Test
  public void testBucketing() {
    // 0 microseconds until 100 seconds
    for (long us = 0; us < 100 * 1000 * 1000; us += 100 ) {
      int bucket = LinkBenchLatency.latencyToBucket(us);
      try {
        assertTrue(bucket >= 0);
        assertTrue(bucket < LinkBenchLatency.NUM_BUCKETS);
        long range[] = LinkBenchLatency.bucketBound(bucket);
        assertTrue(us >= range[0]);
        assertTrue(us < range[1]);
      } catch (AssertionFailedError e) {
        System.err.println("Failed for " + us + "us, bucket=" + bucket);
        throw e;
      }
    }
  }
}
