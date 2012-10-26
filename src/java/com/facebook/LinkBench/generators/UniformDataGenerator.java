package com.facebook.LinkBench.generators;

import java.util.Properties;
import java.util.Random;

import com.facebook.LinkBench.Config;
import com.facebook.LinkBench.ConfigUtil;
import com.facebook.LinkBench.LinkBenchConfigError;

/**
 * A super simple data generator that generates a string of
 * characters chosen uniformly from a range.
 * 
 * This probably isn't a good generator to use if you want something realistic,
 * especially if compressibility properties of the data will affect your 
 * experiment.
 */
public class UniformDataGenerator implements DataGenerator {
  private int range;
  private int start;

  public UniformDataGenerator() {
    start = '\0';
    range = 1;
  }
  
  /**
   * Generate characters from start to end (inclusive both ends)
   * @param start
   * @param end
   */
  public void init(int start, int end) {
    if (start < 0 || start >= 256) {
      throw new LinkBenchConfigError("start " + start +
                                     " out of range [0,255]");
    }
    if (end < 0 || end >= 256) {
      throw new LinkBenchConfigError("endbyte " + end +
                                     " out of range [0,255]");
    }

    if (start >= end) {
      throw new LinkBenchConfigError("startByte " + start 
                                   + " >= endByte " + end);
    }
    this.start = (byte)start;
    this.range = end - start + 1;
  }
  
  @Override
  public void init(Properties props, String keyPrefix) {
    int startByte = ConfigUtil.getInt(props, keyPrefix +
                                     Config.UNIFORM_GEN_STARTBYTE);
    int endByte = ConfigUtil.getInt(props, keyPrefix +
                                     Config.UNIFORM_GEN_ENDBYTE);
    init(startByte, endByte);
  }

  @Override
  public byte[] fill(Random rng, byte[] data) {
    return gen(rng, data, start, range);
  }
  
  public static byte[] gen(Random rng, byte[] data,
                           int startByte, int range) {
    int n = data.length;
    for (int i = 0; i < n; i++) {
      data[i] = (byte) (startByte + rng.nextInt(range));
    }
    return data;
  }

}
