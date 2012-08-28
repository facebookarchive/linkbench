package com.facebook.LinkBench.generators;

import java.util.Random;

/**
 * A super simple data generator that generates a string of
 * characters chosen uniformly from a range.
 * 
 * This probably isn't a good generator to use if you want something realistic,
 * especially if compressibility properties of the data will affect your 
 * experiment.
 */
public class UniformDataGenerator implements DataGenerator {
  private final int range;
  private final byte start;

  /**
   * Generate characters from start to end (inclusive both ends)
   * @param start
   * @param end
   */
  public UniformDataGenerator(byte start, byte end) {
    this.start = start;
    this.range = end - start + 1;
  }
  
  @Override
  public byte[] fill(Random rng, byte[] data) {
    return gen(rng, data, start, range);
  }
  
  public static byte[] gen(Random rng, byte[] data, byte start, int range) {
    int n = data.length;
    for (int i = 0; i < n; i++) {
      data[i] = (byte) (start + rng.nextInt(range));
    }
    return data;
  }

}
