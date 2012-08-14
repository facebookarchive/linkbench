package com.facebook.LinkBench;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.EnhancedPatternLayout;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ConfigUtil {
  public static final String linkbenchHomeEnvVar = "LINKBENCH_HOME";
  public static final String LINKBENCH_LOGGER = "com.facebook.linkbench";
  
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
  
  public static Level getDebugLevel(Properties props)
                                    throws LinkBenchConfigError {
    if (props == null) {
      return Level.DEBUG;
    }
    String levStr = props.getProperty("debuglevel");
    
    if (levStr == null) {
      return Level.DEBUG;
    }
    
    try {
      int level = Integer.parseInt(levStr);
      if (level <= 0) {
        return Level.INFO;
      } else if (level == 1) {
        return Level.DEBUG;
      } else {
        return Level.TRACE;
      }
    } catch (NumberFormatException e) {
      Level lev = Level.toLevel(levStr, null);
      if (lev != null) {
        return lev;
      } else {
        throw new LinkBenchConfigError("Invalid setting for debug level: " + 
                                       levStr);
      } 
    }
  }
  
  /**
   * Setup log4j to log to stderr with a timestamp and thread id
   * Could add in configuration from file later if it was really necessary
   * @param props 
   * @param logFile if not null, info logging will be diverted to this file
   * @throws IOException 
   * @throws Exception 
   */
  public static void setupLogging(Properties props, String logFile)
                                    throws LinkBenchConfigError, IOException {
    Layout fmt = new EnhancedPatternLayout("%p %d [%t]: %m%n%throwable{30}");
    Level logLevel = ConfigUtil.getDebugLevel(props);
    Logger.getRootLogger().removeAllAppenders();
    Logger lbLogger = Logger.getLogger(LINKBENCH_LOGGER);
    lbLogger.setLevel(logLevel);
    ConsoleAppender console = new ConsoleAppender(fmt, "System.err");
    
    /* If logfile is specified, put full stream in logfile and only 
     * print important messages to terminal 
     */
    if (logFile != null) {
      console.setThreshold(Level.WARN); // Only print salient messages
      lbLogger.addAppender(new FileAppender(fmt, logFile));
    }
    lbLogger.addAppender(console);
  }
}
