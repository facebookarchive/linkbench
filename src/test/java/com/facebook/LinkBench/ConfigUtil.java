package com.facebook.LinkBench;

import java.io.File;

public class ConfigUtil {
  public static final String linkbenchHomeEnvVar = "LINKBENCH_HOME";
  
  /**
   * @return null if not set, or if not valid path 
   */
  public static String findLinkBenchHome() {
    String linkBenchHome = System.getenv("LINKBENCH_HOME");
    if (linkBenchHome != null && linkBenchHome.length() > 0) {
      File dir = new File(linkBenchHome);
      if (dir.exists() && dir.isDirectory()) {
        return linkBenchHome;
      }
    }
    return null;
  }
}
