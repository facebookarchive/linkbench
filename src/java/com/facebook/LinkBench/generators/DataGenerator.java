package com.facebook.LinkBench.generators;

import java.util.Random;

public interface DataGenerator {
  /**
   * Fill the provided array with randomly generated data
   * @param data
   * @return the argument, as a convenience so that an array can be
   *    constructed and filled in a single statement
   */
  public byte[] fill(Random rng, byte data[]);
}
