package com.facebook.LinkBench;

import java.util.Random;

import org.junit.Test;

import junit.framework.TestCase;

public class TimerTest extends TestCase {
  
  @Test
  public void testTimer1() {
    for (int i = 0; i < 100; i++) {
      long wakeTime = System.nanoTime() + i * 1000 * 500;
      Timer.waitUntil(wakeTime);
      long now = System.nanoTime();
      assertTrue(now >= wakeTime);
      
      // Check that the precision isn't so awful that it would
      // indicate a definite bug (e.g. 100ms)
      assertTrue(now <= wakeTime + 1e7);
    }
  }
  
  /**
   * Test that we can use the timer to wait for short intervals
   * without getting far behind
   */
  @Test
  public void testTimer2() {
    // Repeatedly wait for 10us
    
    long waits = 100 * 100; // 100ms total
    long time = System.nanoTime();
    long startTime = time;
    for (int i = 0; i < waits; i++) {
      time += 1e4; // 10 us
      Timer.waitUntil(time);
    }
    long endTime = System.nanoTime();
    System.err.println("took " + ((endTime - startTime) / 1000000) + "ms");
    assertTrue(endTime - startTime >= 1e8);
    assertTrue(endTime - startTime < 1.02e8); // no longer than 102ms
  }
  
  @Test
  public void testExponentialArrivals() {
    long randSeed = System.currentTimeMillis();
    System.err.println("Random seed: " + randSeed);
    Random rng = new Random(randSeed);
    
    int trials = 40000;
    int arrivalRate_s = 200000;
    double arrivalRate_ns = arrivalRate_s / (double)1e9;
    
    // Check that the exponential distribution is creating correct arrival rate.
    long startTime = System.nanoTime();
    long time = startTime;
    for (int i = 0; i < trials; i++) {
      time = Timer.waitExpInterval(rng, time, arrivalRate_ns);
    }
    long endTime = System.nanoTime();
    
    double actualArrivalRate_ns = trials / (double) (endTime - startTime);
    System.err.println("actual arrival rate: " 
         + actualArrivalRate_ns * 1e9 + " /s " +
    		" expected " + arrivalRate_s + "/s");
    
    assertTrue(actualArrivalRate_ns >= arrivalRate_ns * 0.95);
    assertTrue(actualArrivalRate_ns <= arrivalRate_ns * 1.05);
  }
}
