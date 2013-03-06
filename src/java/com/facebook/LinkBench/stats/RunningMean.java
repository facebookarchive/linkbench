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
package com.facebook.LinkBench.stats;

/**
 * Keep track of mean in numerically stable way
 * See "Comparison of Several Algorithms for Computing Sample Means and
 *      Variances", Ling, 1974, J. American Stat. Assoc.
 */
public class RunningMean {
  /** Number of samples */
  private long n;

  /** First sample */
  private final double v1;

  /** sum of difference */
  private double running;

  /** initialize with first sample */
  public RunningMean(double v1) {
    super();
    this.v1 = v1;
    this.n = 1;
    this.running = 0.0;
  }

  public void addSample(double vi) {
    n++;
    running += (vi - v1);
  }

  public double mean() {
    return v1 + running / n;
  }

  public long samples() {
    return n;
  }
}
