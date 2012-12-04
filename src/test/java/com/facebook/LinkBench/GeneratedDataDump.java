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

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import com.facebook.LinkBench.generators.DataGenerator;
import com.facebook.LinkBench.generators.MotifDataGenerator;
import com.facebook.LinkBench.generators.UniformDataGenerator;
/**
 * Generate some sample data using data generators, in order to test out compressibility
 * of randomly generated data.
 * 
 * This generates several output files consistent of many generated payload data fields
 * separated by newlines
 */
public class GeneratedDataDump {
  private static final Random rng = new Random();
  
  public static void main(String args[]) {
    String outputDir = "";
    if (args.length == 0) {
      outputDir = ".";
    } else if (args.length == 1) {
      outputDir = args[0];
    } else {
      System.err.println("GeneratedDataDump <output dir>");
      System.exit(1);
    }
    
    
    // Number of bytes per row
    final int objBytes = 256;
    final int assocBytes = 6;
    // Number of rows to generate
    final int objRows = 250000;
    final int assocRows = 10000000;
    writeGeneratedDataFile(outputDir + "/gen-data-motif.txt", makeMotifObj(), objRows, objBytes);
    writeGeneratedDataFile(outputDir + "/gen-data-uniform.txt", makeUniformObj(), objRows, objBytes);
    writeGeneratedDataFile(outputDir + "/gen-data-assoc-motif.txt", makeMotifAssoc(), assocRows, assocBytes);
    writeGeneratedDataFile(outputDir + "/gen-data-assoc-uniform.txt", makeUniformAssoc(), assocRows, assocBytes);
  }

  private static void writeGeneratedDataFile(String outFileName,
      DataGenerator gen, int rows, int bytes) {
    OutputStream out = null;
    try {
      out = new BufferedOutputStream(new FileOutputStream(outFileName));
    } catch (FileNotFoundException e) {
      System.err.println("file " + outFileName + " could not be opened");
      System.exit(1);
    }
    
    
    byte buf[] = new byte[bytes];
    
    try {
      for (int i = 0; i < rows; i++) {
        gen.fill(rng, buf);
        out.write(buf);
        out.write('\n');
      }
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static DataGenerator makeUniformObj() {
    UniformDataGenerator gen = new UniformDataGenerator();
    gen.init(50, 75);
    return gen;
  }

  private static MotifDataGenerator makeMotifObj() {
    MotifDataGenerator gen = new MotifDataGenerator();
    int start = 50;
    int end = 220;
    double uniqueness = 0.63;
    gen.init(start, end, uniqueness);
    return gen;
  }
  
  private static DataGenerator makeUniformAssoc() {
    UniformDataGenerator gen = new UniformDataGenerator();
    gen.init(50, 75);
    return gen;
  }

  private static MotifDataGenerator makeMotifAssoc() {
    MotifDataGenerator gen = new MotifDataGenerator();
    int start = 32;
    int end = 100;
    double uniqueness = 0.225;
    int motifSize = 128;
    gen.init(start, end, uniqueness, motifSize);
    return gen;
  }
}
