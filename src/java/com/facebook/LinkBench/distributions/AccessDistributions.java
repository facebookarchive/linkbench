package com.facebook.LinkBench.distributions;

import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Logger;

import com.facebook.LinkBench.Config;
import com.facebook.LinkBench.ConfigUtil;
import com.facebook.LinkBench.LinkBenchConfigError;
import com.facebook.LinkBench.RealDistribution;
import com.facebook.LinkBench.RealDistribution.DistributionType;
import com.facebook.LinkBench.util.ClassLoadUtil;

/**
 * Module for id access patterns that allows different implementations
 * of the AccessDistribution interface to be instantiated for configurable
 * access patterns.
 * @author tarmstrong
 *
 */
public class AccessDistributions {
  public interface AccessDistribution {
    /**
     * Choose the next id to be accessed
     * @param rng random number generator
     * @param previousId previous ID (for stateful generators)
     * @return
     */
    public abstract long nextID(Random rng, long previousId);
    
    /**
     * A set of parameters to use to shuffle the results, or
     * null if the results shouldn't be shuffled
     * @return
     */
    public abstract long[] getShuffleParams();
  }
  
  public static class BuiltinAccessDistribution implements AccessDistribution {
    private AccessDistMode mode;
    protected long minid;
    protected long maxid;
    private long config;
    
    /** Use to generate decent quality random longs in range */
    UniformDistribution uniform;  
    
    public BuiltinAccessDistribution(AccessDistMode mode,
                            long minid, long maxid, long config) {
      this.mode = mode;
      this.minid = minid;
      this.maxid = maxid;
      this.config = config;
      uniform = new UniformDistribution();
      uniform.init(minid, maxid, null, null);
    }

    @Override
    public long nextID(Random rng, long previousid) {
      long newid;
      double drange = (double)(maxid - minid);
      
      switch(mode) {
      case ROUND_ROBIN: //sequential from startid1 to maxid1 (circular)
        if (previousid <= minid) {
          newid = minid;
        } else {
          newid = previousid+1;
          if (newid > maxid) {
            newid = minid;
          }
        }
        break;

      case RECIPROCAL: // inverse function f(x) = 1/x.
        newid = (long)(Math.ceil(drange/uniform.choose(rng)));
        if (newid < minid) newid = minid;
        if (newid >= maxid) newid = maxid;
        break;
      case MULTIPLE: // generate id1 that is even multiple of config
        newid = config * (long)(Math.ceil(uniform.choose(rng)/config));
        break;
      case POWER: // generate id1 that is power of config
        double log = Math.ceil(Math.log(uniform.choose(rng))/Math.log(config));
        newid = Math.min(maxid - 1, (long)Math.pow(config, log));
        break;
      case PERFECT_POWER: // generate id1 that is perfect square if config is 2,
        // perfect cube if config is 3 etc
        // get the nth root where n = distrconfig
        long nthroot = (long)Math.ceil(Math.pow(uniform.choose(rng), (1.0)/config));
        // get nthroot raised to power n
        newid = Math.min(maxid - 1, (long)Math.pow(nthroot, config));
        break;
      default:
        throw new RuntimeException("Unknown access dist mode: " + mode);
      }
      return newid;
    }

    @Override
    public long[] getShuffleParams() {
      // Don't shuffle these distributions
      return null;
    }
  }
  
  public static class ProbAccessDistribution implements AccessDistribution {
    private final ProbabilityDistribution dist;
    private long[] shuffleParams; 
    
    public ProbAccessDistribution(ProbabilityDistribution dist, 
                                  long shuffleParams[]) {
      super();
      this.dist = dist;
      this.shuffleParams = shuffleParams;
    }

    @Override
    public long nextID(Random rng, long previousId) {
      return dist.choose(rng);
    }

    @Override
    public long[] getShuffleParams() {
      return shuffleParams;
    }

  }
  
  public static enum AccessDistMode {
    REAL, // Real empirical distribution
    ROUND_ROBIN, // Cycle through ids
    RECIPROCAL, // Pick with probability 
    MULTIPLE, // Pick a multiple of config parameter
    POWER, // Pick a power of config parameter
    PERFECT_POWER // Pick a perfect power (square, cube, etc) with exponent
                  // as configured
  }
  
  public static AccessDistribution loadAccessDistribution(Properties props,
      long minid, long maxid, DistributionType kind) throws LinkBenchConfigError {
    Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
    String keyPrefix;
    switch(kind) {
    case READS:
      keyPrefix = Config.READ_CONFIG_PREFIX;
      break;
    case WRITES:
      keyPrefix = Config.WRITE_CONFIG_PREFIX;
      break;
    case NODE_ACCESSES:
      keyPrefix = Config.NODE_ACCESS_CONFIG_PREFIX;
      break;
    default:
      throw new RuntimeException("Bad kind " + kind);
    }
    
    String func_key = keyPrefix + Config.ACCESS_FUNCTION_SUFFIX;
    String access_func = props.getProperty(func_key);

    if (access_func == null) {
      throw new LinkBenchConfigError(func_key + " not defined");
    }
    
    try {
      AccessDistMode mode = AccessDistMode.valueOf(access_func.toUpperCase());

      if (mode == AccessDistMode.REAL) {
        RealDistribution realDist = new RealDistribution();
        realDist.init(props, minid, maxid, kind);
        long shuffleParams[] = RealDistribution.getShuffleParams(kind);
        logger.debug("Using real access distribution" +
                     " for " + kind.toString().toLowerCase());
        return new ProbAccessDistribution(realDist, shuffleParams);
      } else  {
        String config_key = keyPrefix + Config.ACCESS_CONFIG_SUFFIX;
        String config_val_str = props.getProperty(config_key);
        if (config_val_str == null) {
          throw new LinkBenchConfigError(config_key + " not specified");
        }
        long config_val = Long.parseLong(config_val_str);
        logger.debug("Using built-in access distribution " + mode +
                    " with config param " + config_val +
                    " for " + kind.toString().toLowerCase());
        return new BuiltinAccessDistribution(mode, minid, maxid, config_val);
      }
    } catch (IllegalArgumentException e) {
      return tryDynamicLoad(access_func, props, keyPrefix, minid, maxid, kind);
    }
  }

  /**
   * 
   * @param className ProbabilityDistribution class name
   * @param props
   * @param keyPrefix prefix to use for looking up keys in props
   * @param minid
   * @param maxid
   * @return
   */
  private static AccessDistribution tryDynamicLoad(String className,
      Properties props, String keyPrefix, long minid, long maxid,
      DistributionType kind) {
    try {
      Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
      logger.debug("Using ProbabilityDistribution class " + className +
                  " for " + kind.toString().toLowerCase());
      ProbabilityDistribution pDist = ClassLoadUtil.newInstance(className,
                                                ProbabilityDistribution.class);
      pDist.init(minid, maxid, props, keyPrefix);
      long shuffleParams[] = RealDistribution.getShuffleParams(kind);
      return new ProbAccessDistribution(pDist, shuffleParams);
    } catch (ClassNotFoundException e) {
      throw new LinkBenchConfigError("Access distribution class " + className
          + " not successfully loaded: " + e.getMessage());
    }
  }
}
