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

import java.util.Arrays;

import org.junit.Test;

import junit.framework.TestCase;
/**
 * Check that the shuffler obeys expected invariants
 */
public class InvertibleShufflerTest extends TestCase {


  @Test
  public void testShuffleSmallRange() {
    long seed = 1234;
    testShuffle(0, 10, true, seed, 1);
    testShuffle(0, 4, true, seed, 2);
    testShuffle(0, 10, true, seed, 5);
    testShuffle(0, 7, true, seed, 3);
    testShuffle(0, 10, true, seed, 7);
    testShuffle(0, 10, true, seed, 11);
    testShuffle(0, 10, true, seed, 15);
    testShuffle(0, 100, true, seed, 11);
  }

  @Test
  public void testShuffleMedRange() {
    testShuffle(512, 10543, false, 13, 7);
    testShuffle(512, 10543, false, 13, 7, 27, 140);
  }

  @Test
  public void testShuffleLargeRange() {
    testShuffle(12345, 123456, false, 13, 7, 27, 140);
  }

  /**
   * Check that result is a valid permutation (i.e. a 1->1 mapping)
   * @param minId
   * @param maxId
   * @param params
   */
  public static void testShuffle(int minId, int maxId, boolean print,
                                 long... params) {
    String shuffleDesc = String.format(
          "Permuting range [%d,%d) with params %s", minId, maxId,
          Arrays.toString(params));

    if (print) {
      System.err.println(shuffleDesc);
    }


    int n = maxId - minId;


    //ProbDistShuffler shuf = new ProbDistShuffler(params[0], (int)params[1], n);
    InvertibleShuffler shuf = new InvertibleShuffler(params[0], (int)params[1], n);

    long reverse[] = new long[n]; // Store the reverse permutation
    // Store if ID has appeared (inited to false)
    boolean exists[] = new boolean[n];

    for (int i = minId; i < maxId; i++) {
      //long lj = Shuffler.getPermutationValue(i, minId, maxId, params);
      long lj = minId + shuf.permute(i - minId);
      assertEquals(i, minId + shuf.invertPermute(lj - minId));
      if (lj < minId || lj >= maxId) {
        fail(String.format("Error with test %s, permutation result p(%d) = %d"
            + " out of range [%d, %d)", shuffleDesc, i, lj, minId, maxId));
      }
      assertTrue(lj >= minId);
      assertTrue(lj < maxId);
      int j = (int)lj; // Must be in integer range
      if (exists[j - minId]) {
        fail(String.format(
            "Error with test %s: collision. p(%d) = p(%d) = %d",
            shuffleDesc, i, reverse[j - minId], j));
      }
      reverse[j - minId] = i;
      exists[j - minId] = true;
      if (print) {
        System.err.print(" " + j);
      }
    }
    if (print) {
      System.err.println();
    }
    /* If we made it to here there were no collisions and we know all
     * n ids appeared in the permutation */
  }
}
