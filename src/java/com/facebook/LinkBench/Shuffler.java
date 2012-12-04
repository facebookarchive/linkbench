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


/* 
 * A class to generate permutation of 0, 1, 2, ..(N-1) using O(1) memory.
 */

class Shuffler {
  /* Example to show how algorithm works: n=9; m=4.
   * 
   * Starting with the original a = {0, 1, ..n}, apply the following 
   * transformation to generate a permutation of a:
   *
   * 1. Divide 0..9 into multiple groups, each group has length m=4
   * a = 0 1 2 3|4 5 6 7|8 9
   * 
   * 2. Move elements with same position in each group together
   * M(a) = 0 4 8|1 5 9|2 6|3 7
   * 
   * 3. T(a) = position of i in the permutation in M(a)
   * T(a) = {0 3 6 8 1 4 7 9 2 5}
   */

  //get T(a)[i]
  static long getPermutationValue(long i, long n, long m) {
    long minsize = n/m;
    long maxsize = (n%m == 0) ? minsize : minsize + 1;
    long n_maxsize_groups = n%m;
    long newgroupid = i%m, newidx = i/m;
    if (newgroupid < n_maxsize_groups) {
      return newgroupid*maxsize + newidx;
    }
    else {
      return n_maxsize_groups*maxsize + 
        (newgroupid - n_maxsize_groups)*minsize + newidx;
    }
  }

  static long getPermutationValue(long i, long start, long end, long m) {
    return start + getPermutationValue(i - start, end - start, m);
  }

  //multiplication of transformations
  //apply multiple transformation T to make a more random permutation
  static long getPermutationValue(long i, long start, long end, long[] ms) {
    for (int j = 0; j < ms.length; ++j) { 
      i = getPermutationValue(i, start, end, ms[j]);
    }
    return i;
  }
 }

