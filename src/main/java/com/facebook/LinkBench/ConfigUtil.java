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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.EnhancedPatternLayout;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ConfigUtil {
  public static final String linkbenchHomeEnvVar = "LINKBENCH_HOME";
  public static final String LINKBENCH_LOGGER = "com.facebook.linkbench";

  private static final Logger logger = Logger.getLogger("ConfigUtil");

  public static final String CONFIG_WORKLOAD_FILENAME = "linkbench_workload_filename";
  public static final String CONFIG_DISTRIBUTION_FILENAME = "linkbench_distribution_filename";
  public static final String CONFIG_CONFIG_FILENAME = "linkbench_config_filename";
  public static final String CONFIG_MAPREDUCE_MODE = "linkbench_mapreduce_mode";
  public static final String CONFIG_LOCAL_CACHE_DIST_FILE = "linkbench_local_cache_config_file";

  public static final int CONFIGFILEPOS = 0;
  public static final int WORKLOADFILEPOS = 1;
  public static final int DISTFILEPOS = 2;

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
    String levStr = props.getProperty(Config.DEBUGLEVEL);

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

  /**
   * Look up key in props, failing if not present
   * @param props
   * @param key
   * @return
   * @throws LinkBenchConfigError thrown if key not present
   */
  public static String getPropertyRequired(Properties props, String key)
    throws LinkBenchConfigError {
    String v = props.getProperty(key);
    if (v == null) {
      throw new LinkBenchConfigError("Expected configuration key " + key +
                                     " to be defined");
    }
    return v;
  }

  public static int getInt(Properties props, String key)
      throws LinkBenchConfigError {
    return getInt(props, key, null);
  }

  /**
   * Retrieve a config key and convert to integer
   * @param props
   * @param key
   * @return a non-null string value
   * @throws LinkBenchConfigError if not present or not integer
   */
  public static int getInt(Properties props, String key, Integer defaultVal)
      throws LinkBenchConfigError {
    if (defaultVal != null && !props.containsKey(key)) {
      return defaultVal;
    }
    String v = getPropertyRequired(props, key);
    try {
      return Integer.parseInt(v);
    } catch (NumberFormatException e) {
      throw new LinkBenchConfigError("Expected configuration key " + key +
                " to be integer, but was '" + v + "'");
    }
  }

  public static long getLong(Properties props, String key)
      throws LinkBenchConfigError {
    return getLong(props, key, null);
  }

  /**
   * Retrieve a config key and convert to long integer
   * @param props
   * @param key
   * @param defaultVal default value if key not present
   * @return
   * @throws LinkBenchConfigError if not present or not integer
   */
  public static long getLong(Properties props, String key, Long defaultVal)
      throws LinkBenchConfigError {
    if (defaultVal != null && !props.containsKey(key)) {
      return defaultVal;
    }
    String v = getPropertyRequired(props, key);
    try {
      return Long.parseLong(v);
    } catch (NumberFormatException e) {
      throw new LinkBenchConfigError("Expected configuration key " + key +
                " to be long integer, but was '" + v + "'");
    }
  }


  public static double getDouble(Properties props, String key)
                throws LinkBenchConfigError {
    return getDouble(props, key, null);
  }

  /**
   * Retrieve a config key and convert to double
   * @param props
   * @param key
   * @param defaultVal default value if key not present
   * @return
   * @throws LinkBenchConfigError if not present or not double
   */
  public static double getDouble(Properties props, String key,
        Double defaultVal) throws LinkBenchConfigError {
    if (defaultVal != null && !props.containsKey(key)) {
      return defaultVal;
    }
    String v = getPropertyRequired(props, key);
    try {
      return Double.parseDouble(v);
    } catch (NumberFormatException e) {
      throw new LinkBenchConfigError("Expected configuration key " + key +
                " to be double, but was '" + v + "'");
    }
  }

  /**
   * Retrieve a config key and convert to boolean.
   * Valid boolean strings are "true" or "false", case insensitive
   * @param props
   * @param key
   * @return
   * @throws LinkBenchConfigError if not present or not boolean
   */
  public static boolean getBool(Properties props, String key)
      throws LinkBenchConfigError {
    String v = getPropertyRequired(props, key).trim().toLowerCase();
    // Parse manually since parseBoolean accepts many things as "false"
    if (v.equals("true")) {
      return true;
    } else if (v.equals("false")) {
      return false;
    } else {
      throw new LinkBenchConfigError("Expected configuration key " + key +
                " to be true or false, but was '" + v + "'");
    }
  }
  
  
  /**
   * @param configFile
   * @param fileNames, array[3] to hold the parsed config files' name.
   * @return
   * @throws IOException
   * @throws FileNotFoundException
   */
  public static Properties parseConfigFile(String configFile, String[] fileNames) throws IOException, FileNotFoundException {

    // check the fileNames array size, ugly! could use better implementation.
    if (fileNames.length < 3) {
      throw new RuntimeException("fileNames.length < 3");
    }

    logger.info("configFile = " + configFile);
    Properties props = new Properties();
    props.load(new FileInputStream(configFile));

    String workloadConfigFile = null;
    if (props.containsKey(Config.WORKLOAD_CONFIG_FILE)) {
      workloadConfigFile = props.getProperty(Config.WORKLOAD_CONFIG_FILE);
      if (!new File(workloadConfigFile).isAbsolute()) {
        String linkBenchHome = ConfigUtil.findLinkBenchHome();
        if (linkBenchHome == null) {
          throw new RuntimeException("Data file config property "
              + Config.WORKLOAD_CONFIG_FILE
              + " was specified using a relative path, but linkbench home"
              + " directory was not specified through environment var "
              + ConfigUtil.linkbenchHomeEnvVar);
        } else {
          workloadConfigFile = linkBenchHome + File.separator + workloadConfigFile;
        }
      }

      logger.info("workloadConfigFile = " + workloadConfigFile);
      Properties workloadProps = new Properties();
      workloadProps.load(new FileInputStream(workloadConfigFile));
      
      // Add workload properties, but allow other values to override
      for (String key: workloadProps.stringPropertyNames()) {
        if (props.getProperty(key) == null) {
          props.setProperty(key, workloadProps.getProperty(key));
        }
      }
    }

    String distributionDataFile = null;

    if (props.containsKey(Config.DISTRIBUTION_DATA_FILE)) {
      distributionDataFile = ConfigUtil.getPropertyRequired(props,
          Config.DISTRIBUTION_DATA_FILE);
    }

    logger.info("distributionDataFile = " + distributionDataFile);

    fileNames[CONFIGFILEPOS] = configFile != null ? new Path(configFile).getName() : null;
    fileNames[WORKLOADFILEPOS] = workloadConfigFile != null ? new Path(workloadConfigFile).getName() : null;
    fileNames[DISTFILEPOS] = distributionDataFile != null ? new Path(distributionDataFile).getName() : null;

    return props;
  }

  
  public static void setConfigFileNamesToConf(Configuration conf, String fileNames[]) {
    assert(fileNames.length >= 3);
    conf.set(CONFIG_CONFIG_FILENAME, fileNames[CONFIGFILEPOS]);
    conf.set(CONFIG_WORKLOAD_FILENAME, fileNames[WORKLOADFILEPOS]);
    conf.set(CONFIG_DISTRIBUTION_FILENAME, fileNames[DISTFILEPOS]);
  }

  public static String[] getConfigFileNamesFromConf(Configuration conf) {
    String fileNames[] = new String[3];

    fileNames[CONFIGFILEPOS] = conf.get(CONFIG_CONFIG_FILENAME);
    fileNames[WORKLOADFILEPOS] = conf.get(CONFIG_WORKLOAD_FILENAME);
    fileNames[DISTFILEPOS] = conf.get(CONFIG_DISTRIBUTION_FILENAME);
    
    return fileNames;
  }
  
  /**
   * Loading properties from files on Distributed cache directly.
   * @param configFile
   * @return
   * @throws IOException
   * @throws FileNotFoundException
   */
  public static Properties loadPropertiesMR(Configuration conf) throws IOException, FileNotFoundException {

    String configFileNames[] = getConfigFileNamesFromConf(conf);

    Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

    String localConfigFiles[] = new String[3];
    
    for (Path p: localFiles) {
      logger.info("file = " + p.getName());
      if (p.getName().equalsIgnoreCase(configFileNames[CONFIGFILEPOS])) {
        localConfigFiles[CONFIGFILEPOS] = p.toString();
      } else if (p.getName().equalsIgnoreCase(configFileNames[WORKLOADFILEPOS])) {
        localConfigFiles[WORKLOADFILEPOS] = p.toString();
      } else if (p.getName().equalsIgnoreCase(configFileNames[DISTFILEPOS])) {
        localConfigFiles[DISTFILEPOS] = p.toString();
      }
    }

    Properties props = new Properties();
    props.load(new FileInputStream(localConfigFiles[CONFIGFILEPOS]));
    
    String workloadConfigFile = localConfigFiles[WORKLOADFILEPOS];
    if (workloadConfigFile != null) {
      Properties workloadProps = new Properties();
      workloadProps.load(new FileInputStream(workloadConfigFile));
      
      // Add workload properties, but allow other values to override
      for (String key: workloadProps.stringPropertyNames()) {
        if (props.getProperty(key) == null) {
          props.setProperty(key, workloadProps.getProperty(key));
        }
      }
    }

    props.setProperty(CONFIG_MAPREDUCE_MODE, "TRUE");
    props.setProperty(CONFIG_LOCAL_CACHE_DIST_FILE, localConfigFiles[DISTFILEPOS]);

    return props;
  }

  public static Boolean isMapReduceMode(Properties props) {
    Boolean result = false;
    try {
      result = getBool(props, CONFIG_MAPREDUCE_MODE);
    } catch (LinkBenchConfigError e) {
      // The property is not existing, thus not in MapReduce Mode.
      result = false;
    }
    return result;
  }
}
