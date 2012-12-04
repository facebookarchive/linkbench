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
