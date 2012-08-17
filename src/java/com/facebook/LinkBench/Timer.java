package com.facebook.LinkBench;

import java.util.Random;

public class Timer {
  
  /**
   * Wait an amount of time since the last event determined by the
   * exponential distribution
   * @param rng random number generator to use
   * @param lastevent_time_ns last event time (units same as System.nanoTime())
   * @param arrival_rate_ns arrival rate: events per nanosecond
   * @return time of the next event
   */
  public static long waitExpInterval(Random rng,
                long lasteventTime_ns, double arrivalRate_ns) {
    long nextTime_ns = lasteventTime_ns +
        Math.round(-1 * Math.log(rng.nextDouble()) / arrivalRate_ns);
    Timer.waitUntil(nextTime_ns);
    return nextTime_ns;
  }
  
  /**
   * Wait until System.nanoTime() is > the argument
   * @param time_ns
   */
  public static void waitUntil(long time_ns) {
    long now = System.nanoTime();
    while (now < time_ns) {
      long wait = time_ns - now;
      try {
        Thread.sleep(wait / 1000000, (int)(wait % 1000));
      } catch (InterruptedException ie) {
        // Restart loop
      }
      now = System.nanoTime();
    }
  }
}
