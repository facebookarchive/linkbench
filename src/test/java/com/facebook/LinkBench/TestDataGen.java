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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import junit.framework.TestCase;

import org.junit.Test;

import com.facebook.LinkBench.generators.DataGenerator;
import com.facebook.LinkBench.generators.MotifDataGenerator;
import com.facebook.LinkBench.generators.UniformDataGenerator;

public class TestDataGen extends TestCase {

  public static void printByteGrid(byte[] data) {
    for (int i = 0; i < data.length; i += 32) {
      for (int j = i; j < Math.min(i + 32, data.length); j++) {
        System.err.format("%3d ", data[j]);
      }
      System.err.println();
    }
  }

  /**
   * Test how quickly uniform data generator can generate patterns
   */
  @Test
  public void testTimingUniform() {
    System.err.println("Testing uniform generator");
    System.err.println("=========================");
    DataGenFactory fact = new DataGenFactory() {
      public DataGenerator make(double param) {
        UniformDataGenerator gen = new UniformDataGenerator();
        gen.init(0,  8);
        return gen;
      }
    };
    testTiming(fact, 128);
    testTiming(fact, 1024);
  }

  /**
   * Test how quickly motif data generator can generate patterns to make
   * sure its not
   */
  @Test
  public void testTimingMotif() {
    System.err.println("Testing motif generator");
    System.err.println("=========================");
    DataGenFactory fact = new DataGenFactory() {
      public DataGenerator make(double param) {
        MotifDataGenerator gen = new MotifDataGenerator();
        gen.init(0,  8, param);
        return gen;
      }
    };
    testTiming(fact, 128);
    testTiming(fact, 1024);
  }

  private void testTiming(DataGenFactory fact, int bufSize) {
    byte buf[] = new byte[bufSize];
    Random rng = new Random();

    int trials = 200000;
    double params[] = new double[] {0.0, 0.25, 0.5, 0.75, 1.0};
    long times_ns[] = new long[params.length];
    for (int i = 0; i < params.length; i++) {
      double param = params[i];
      // Warm up
      doTest(fact, buf, rng, trials, param);
      // Make sure hotspot will have compiled
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      long timeTaken = doTest(fact, buf, rng, trials, param);
      times_ns[i] = timeTaken;
    }

    for (int i = 0; i < params.length; i++) {
      double trialTime = times_ns[i] / (double) trials;
      double byteTime = trialTime / buf.length;
      System.err.format("uniqueness = %.3f, time for %d byte buffer = %.1f ns, time per byte = %.1fns\n",
                  params[i], buf.length, trialTime, byteTime);

    }
  }

  private static interface DataGenFactory {
    public abstract DataGenerator make(double param);
  }

  private long doTest(DataGenFactory fact, byte[] buf, Random rng, int trials, double param) {
    DataGenerator gen = fact.make(param);

    long start = System.nanoTime();
    for (int j = 0; j < trials; j++) {
      gen.fill(rng, buf);
    }
    long end = System.nanoTime();
    long timeTaken = end - start;
    return timeTaken;
  }

  /**
   * Exercise the motif data generator and print the output.
   *
   * Currently difficult to automatically verify output.
   */
  @Test
  public void testMotif() {
    MotifDataGenerator gen = new MotifDataGenerator();

    System.err.println("uniqueness 0.25");
    gen.init(0, 8, 0.25);
    byte data[] = gen.fill(new Random(), new byte[64]);
    printByteGrid(data);

    System.err.println("uniqueness 0.0");
    gen.init(0, 8, 0.0);
    data = gen.fill(new Random(), new byte[64]);
    printByteGrid(data);

    System.err.println("uniqueness 0.05");
    gen.init(0, 8, 0.05);
    data = gen.fill(new Random(), new byte[64]);
    printByteGrid(data);

    System.err.println("uniqueness 1.0");
    gen.init(0, 8, 1.0);
    data = gen.fill(new Random(), new byte[64]);
    printByteGrid(data);
  }

  /**
   * Estimate the compressibility of randomly generated data by
   * compressing a long stream of the data
   * @throws IOException
   */
  @Test
  public void testCompressibility() throws IOException {
    MotifDataGenerator gen = new MotifDataGenerator();
    gen.init(0, 255, 0.5);
    System.err.println("\nUniqueness=0.5 Range=255\n===============");
    testCompressibility(gen, 1024, 10000);
    testCompressibility(gen, 64, 1);

    gen.init(0, 127, 0.5);
    System.err.println("\nUniqueness=0.5 Range=127\n===============");
    testCompressibility(gen, 1024, 10000);
    testCompressibility(gen, 64, 1);

    gen.init(0, 255, 0.0);
    System.err.println("\nUniqueness=0.0 Range=255\n===============");
    testCompressibility(gen, 1024, 10000);
    testCompressibility(gen, 64, 1);

    gen.init(0, 255, 1.0);
    System.err.println("\nUniqueness=1.0 Range=255\n===============");
    testCompressibility(gen, 1024, 10000);
    testCompressibility(gen, 64, 1);
    gen.init(0, 255, 1.0);

    gen.init(0, 127, 1.0);
    System.err.println("\nUniqueness=1.0 Range=127\n===============");
    testCompressibility(gen, 1024, 10000);
    testCompressibility(gen, 64, 1);

    gen.init(0, 1, 1.0);
    System.err.println("\nUniqueness=1.0 Range=1\n===============");
    testCompressibility(gen, 1024, 10000);
    testCompressibility(gen, 64, 1);
  }

  private void testCompressibility(MotifDataGenerator gen, int blockSize, int blocks)
                                                              throws IOException {
    long seed = System.nanoTime();
    System.err.println("seed = " + seed);
    Random rng = new Random(seed);
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    Deflater def = new Deflater(Deflater.BEST_COMPRESSION);
    DeflaterOutputStream gzipOut = new DeflaterOutputStream(byteOut, def);

    byte block[] = new byte[blockSize];
    for (int i = 0; i < blocks; i++) {
      gen.fill(rng, block);
      gzipOut.write(block);
    }
    gzipOut.close();

    byte compressed[] = byteOut.toByteArray();
    int origLen = blockSize * blocks;
    System.err.format("%dx%d blocks.  Compressed %d bytes to %d: %.2f.  Bound: %.2f\n",
            blocks, blockSize,
            origLen, compressed.length, compressed.length / (double) origLen,
            gen.estMaxCompression());
  }
}
