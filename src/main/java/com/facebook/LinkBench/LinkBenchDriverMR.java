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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.facebook.LinkBench.LinkBenchLoad.LoadChunk;
import com.facebook.LinkBench.LinkBenchLoad.LoadProgress;
import com.facebook.LinkBench.LinkBenchRequest.RequestProgress;
import com.facebook.LinkBench.stats.LatencyStats;
import com.facebook.LinkBench.util.ClassLoadUtil;


/**
 * LinkBenchDriverMR class.
 * First loads data using map-reduced LinkBenchLoad class.
 * Then does read and write requests of various types (addlink, deletelink,
 * updatelink, getlink, countlinks, getlinklist) using map-reduced
 * LinkBenchRequest class.
 * Config options are taken from config file passed as argument.
 */

public class LinkBenchDriverMR extends Configured implements Tool {
  public static final int LOAD = 1;
  public static final int REQUEST = 2;
  public static final int NODEMAPPER = 0;
  public static final int LINKMAPPER = 1;
  public static final int REQUESTMAPPER = 0;

  private static Path TMP_DIR = new Path("TMP_Link_Bench");
  private static boolean REPORT_PROGRESS = false;

  private static final Logger logger =
              Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

  static enum Counters { NODE_LOADED, LINK_LOADED, REQUEST_DONE }

  private Properties props;
  private String[] configFileNames = new String[3];
  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};

  private static void enqueueLoadWork(BlockingQueue<LoadChunk> chunk_q,
      long startid1, long maxid1, int chunkSize, Random rng) {
    // Enqueue work chunks.  Do it in reverse order as a heuristic to improve
    // load balancing, since queue is FIFO and later chunks tend to be larger

    long chunk_num = 0;
    ArrayList<LoadChunk> stack = new ArrayList<LoadChunk>();
    for (long id1 = startid1; id1 < maxid1; id1 += chunkSize) {
      stack.add(new LoadChunk(chunk_num, id1,
                    Math.min(id1 + chunkSize, maxid1), rng));
      chunk_num++;
    }

    for (int i = stack.size() - 1; i >= 0; i--) {
      chunk_q.add(stack.get(i));
    }

    chunk_q.add(LoadChunk.SHUTDOWN);
  }
  
  /**
   * generate an instance of LinkStore
   * @param currentphase LOAD or REQUEST
   * @param mapperid id of the mapper 0, 1, ...
   */
  private static LinkStore initStore(String linkStoreClassName, Phase currentphase, int mapperid)
    throws IOException {

    LinkStore newstore = null;

    logger.info("Using store class: " + linkStoreClassName);

    // The property "store" defines the class name that will be used to
    // store data in a database. The folowing class names are pre-packaged
    // for easy access:
    //   LinkStoreMysql :  run benchmark on  mySQL
    //   LinkStoreHBase :  run benchmark on  HBase
    //   LinkStoreHBaseGeneralAtomicityTesting : atomicity testing on HBase.
    //   LinkStoreTaoAtomicityTesting:  atomicity testing for Facebook's HBase
    //
    Class<?> clazz = null;
    try {
      clazz = getClassByName(linkStoreClassName);
    } catch (java.lang.ClassNotFoundException nfe) {
      throw new IOException("Cound not find class for " + linkStoreClassName);
    }
    newstore = (LinkStore)newInstance(clazz);
    if (clazz == null) {
      throw new IOException("Unknown data store " + linkStoreClassName);
    }

    return newstore;
  }

  /**
   * @param linkStore a LinkStore instance to be reused if it turns out
   * that linkStore and nodeStore classes are same
   * @return
   * @throws Exception
   * @throws IOException
   */
  private static NodeStore createNodeStore(String nodeStoreClassName, LinkStore linkStore) throws
      IOException {
    if (nodeStoreClassName == null) {
      logger.debug("No NodeStore implementation provided");
    } else {
      logger.debug("Using NodeStore implementation: " + nodeStoreClassName);
    }

    if (linkStore != null && linkStore.getClass().getName().equals(
                                                nodeStoreClassName)) {
      // Same class, reuse object
      if (!NodeStore.class.isAssignableFrom(linkStore.getClass())) {
        throw new IOException("Specified NodeStore class " + nodeStoreClassName
                          + " is not a subclass of NodeStore");
      }
      return (NodeStore)linkStore;
    } else {
      NodeStore nodeStore;
      try {
        nodeStore = ClassLoadUtil.newInstance(nodeStoreClassName,
                                                            NodeStore.class);
        return nodeStore;
      } catch (java.lang.ClassNotFoundException nfe) {
        throw new IOException("Cound not find class for " + nodeStoreClassName);
      }
    }
  }
  
  /**
   * generate an instance of NodeStore
   * @return
   * @throws Exception
   */
  private static NodeStore createNodeStore(String nodeStoreClassName) throws IOException {
    if (nodeStoreClassName == null) {
      logger.debug("No NodeStore implementation provided");
    } else {
      logger.debug("Using NodeStore implementation: " + nodeStoreClassName);
    }

    NodeStore nodeStore;
    try {
      nodeStore = ClassLoadUtil.newInstance(nodeStoreClassName,
                                                          NodeStore.class);
      return nodeStore;
    } catch (java.lang.ClassNotFoundException nfe) {
      throw new IOException("Cound not find class for " + nodeStoreClassName);
    }
  
  }
  
   /**
   * create JobConf for map reduce job
   * @param currentphase LOAD or REQUEST
   * @param nmappers number of mappers (loader or requester)
   * @param nnodemappers number of mappers for node ( only work for loader )
   */
/*
  private JobConf createJobConf(int currentphase, int nmappers, int nnodemappers) {
    final JobConf jobconf = new JobConf(getConf(), getClass());
    jobconf.setJobName("LinkBench MapReduce Driver");

    if (USE_INPUT_FILES) {
      jobconf.setInputFormat(SequenceFileInputFormat.class);
    } else {
      jobconf.setInputFormat(LinkBenchInputFormat.class);
    }
    jobconf.setOutputKeyClass(IntWritable.class);
    jobconf.setOutputValueClass(LongWritable.class);
    jobconf.setOutputFormat(SequenceFileOutputFormat.class);
    if(currentphase == LOAD) {
      jobconf.setMapperClass(LoadMapper.class);
    } else { //REQUEST
      jobconf.setMapperClass(RequestMapper.class);
    }
    jobconf.setNumMapTasks(nmappers + nnodemappers);
    jobconf.setReducerClass(LoadRequestReducer.class);
    jobconf.setNumReduceTasks(1);

    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    jobconf.setSpeculativeExecution(false);

    return jobconf;
  }
*/

  /**
  * Prepare map reduce job
  * @param currentphase LOAD or REQUEST
  * @param nmappers number of mappers (loader or requester)
  * @param nnodemappers number of mappers for node ( only work for loader )
  * @throws IOException 
  */
  
  private Job prepareJob(int currentphase, int nmappers, int nnodemappers)
      throws IOException {
    
    Configuration conf = getConf();
    ConfigUtil.setConfigFileNamesToConf(conf, configFileNames);

    Job job = new Job(conf);
    
    job.setJarByClass(LinkBenchDriverMR.class);
    job.setJobName("LinkBench MapReduce Driver");

    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(LongWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    if(currentphase == LOAD) {
      job.setMapperClass(LoadMapper.class);
    } else { //REQUEST
      job.setMapperClass(RequestMapper.class);
    }
    
    job.setReducerClass(LoadRequestReducer.class);
    job.setNumReduceTasks(1);

    TableMapReduceUtil.addDependencyJars(job);
    // Add a Class from the hbase.jar so it gets registered too.
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
        org.apache.hadoop.hbase.util.Bytes.class);

//    TableMapReduceUtil.initCredentials(job);


    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    job.setSpeculativeExecution(false);

    return job;
  }
  
  /**
   * setup input files for map reduce job
   * @param jobconf configuration of the map reduce job
   * @param nmappers number of mappers (loader or requester)
   * @param nnodemappers number of mappers for node ( only work for loader )
   */
  private FileSystem setupInputFiles(Job job, int currentphase, int nmappers, int nnodemappers)
    throws IOException, InterruptedException {
    //setup input/output directories
    final Path indir = new Path(TMP_DIR, "in");
    final Path outdir = new Path(TMP_DIR, "out");
    FileInputFormat.setInputPaths(job, indir);
    FileOutputFormat.setOutputPath(job, outdir);

    final FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(TMP_DIR)) {
      throw new IOException("Tmp directory " + fs.makeQualified(TMP_DIR)
          + " already exists.  Please remove it first.");
    }
    if (!fs.mkdirs(indir)) {
      throw new IOException("Cannot create input directory " + indir);
    }

    //generate an input file for each map task

    // For Link
    for (int i = 0; i < nmappers; ++i) {
      final Path file = new Path(indir, "part" + i);
      final IntWritable mapperid = new IntWritable(i);
      final IntWritable nummappers = new IntWritable(nmappers);
      final SequenceFile.Writer writer = SequenceFile.createWriter(fs, getConf(),
          file, IntWritable.class, IntWritable.class, CompressionType.NONE);
      try {
        writer.append(mapperid, nummappers);
      } finally {
        writer.close();
      }
      logger.info("Wrote input for Map #" + i);
    }

    if (currentphase == LOAD) {
      // Use negative ID for node to distinguish from link. dirty but quick ;)
      // start from -1 to -nnodemappers
      for (int i = 0; i < nnodemappers; ++i) {

        // need to add the offest with nmappers
        final Path file = new Path(indir, "part" + (i + nmappers));

        final IntWritable mapperid = new IntWritable(-1 - i);
        final IntWritable nummappers = new IntWritable(nnodemappers);
        final SequenceFile.Writer writer = SequenceFile.createWriter(fs,
            getConf(), file, IntWritable.class, IntWritable.class,
            CompressionType.NONE);
        try {
          writer.append(mapperid, nummappers);
        } finally {
          writer.close();
        }
        logger.info("Wrote input for Node Map #" + i);
      }
    }

    return fs;
  }

  /**
   * read output from the map reduce job
   * @param fs the DFS FileSystem
   * @param the map reduce job
   */
  public long[] readOutput(int currentphase, FileSystem fs, Job job)
    throws IOException, InterruptedException {
    //read outputs
    final Path outdir = new Path(TMP_DIR, "out");
    Path infile = new Path(outdir, "reduce-out");
    IntWritable type = new IntWritable();
    LongWritable result = new LongWritable();
    long output[] = new long[2];
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, infile, getConf());
    try {
      if (currentphase == LOAD) {
        reader.next(type, result);
        output[NODEMAPPER] = result.get();
        reader.next(type, result);
        output[LINKMAPPER] = result.get();
      } else if (currentphase == REQUEST) {
        reader.next(type, result);
        output[REQUESTMAPPER] = result.get();
      }
    } finally {
      reader.close();
    }
    return output;
  }

  /**
   * Mapper for LOAD phase
   * Load data to the store
   * Output the number of loaded links
   */
  public static class LoadMapper extends 
    Mapper<IntWritable, IntWritable, IntWritable, LongWritable> {
    private Configuration conf;
    private Properties props;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      conf = context.getConfiguration();
      props = ConfigUtil.loadPropertiesMR(conf);
      //ConfigUtil.setupLogging(props, null);
    }

    @Override
    public void map(IntWritable loaderid,
                    IntWritable nloaders,
                    Context context)
           throws IOException, InterruptedException{
     
      int id = loaderid.get();
      int numMapper = nloaders.get();

      boolean mapperForNode = id < 0 ? true : false;
      if (mapperForNode) {
        id = (-1 - id); // Map from -1 ~ -n back to ( 0 ~ n-1);
      }

      long maxid1 = ConfigUtil.getLong(props, Config.MAX_ID);
      long startid1 = ConfigUtil.getLong(props, Config.MIN_ID);

      LoadProgress prog_tracker = LoadProgress.create(
            Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER), props);

      // FIXME : need to detect the case that mapperStep < 1 here
      // or do not allow this to happen when create the job.
      long mapperStep = (maxid1 - startid1) / numMapper;
      long mapperStartId = startid1 + (id * mapperStep);
      long mapperEndId = mapperStartId + mapperStep;
      
      // The last mapper should cover the range up to maxid1;
      if (id == numMapper -1) {
        mapperEndId = maxid1;
      }

      logger.info("Map Task " + id + ", ID range : [" + mapperStartId + ", " + mapperEndId + ")");
      //Fixme : in MR mode, latencyStats array approaching not working right, need a fix.
      LatencyStats latencyStats = new LatencyStats(numMapper);

      if (mapperForNode) {
        String nodeStoreClassName = props.getProperty(Config.NODESTORE_CLASS);
        // for Node
        NodeStore nodeStore = createNodeStore(nodeStoreClassName);
        Random rng = new Random();
        NodeLoaderForMR loader = new NodeLoaderForMR(props, logger, nodeStore, rng,
            latencyStats, null, id, mapperStartId, mapperEndId);
        loader.run();
        long nodesloaded = loader.getNodesLoaded();
        context.write(new IntWritable(NODEMAPPER), new LongWritable(nodesloaded));
        if (REPORT_PROGRESS) {
          context.getCounter(Counters.NODE_LOADED).increment(nodesloaded);
        }

      } else {
        // for Link
        LinkStore store = initStore(
            ConfigUtil.getPropertyRequired(props, Config.LINKSTORE_CLASS),
            Phase.LOAD, id);

        int chunkSize = ConfigUtil.getInt(props, Config.LOADER_CHUNK_SIZE, 2048);
        BlockingQueue<LoadChunk> chunk_q = new LinkedBlockingQueue<LoadChunk>();
        enqueueLoadWork(chunk_q, mapperStartId, mapperEndId, chunkSize, new Random());
     
        LinkBenchLoad loader = new LinkBenchLoad(store, props, latencyStats,
                                 null,
                                 id, false,
                                 chunk_q, prog_tracker);
        prog_tracker.startTimer();
        loader.run();
        long linksloaded = loader.getLinksLoaded();

        context.write(new IntWritable(LINKMAPPER), new LongWritable(linksloaded));
        if (REPORT_PROGRESS) {
          context.getCounter(Counters.LINK_LOADED).increment(linksloaded);
        }
      }

      context.progress();
    }
  }

  /**
   * Mapper for REQUEST phase
   * Send requests
   * Output the number of finished requests
   */
  public static class RequestMapper
    extends Mapper<IntWritable, IntWritable, IntWritable, LongWritable> {

    private Configuration conf;
    private Properties props;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      conf = context.getConfiguration();
      props = ConfigUtil.loadPropertiesMR(conf);
    }

    @Override
    public void map(IntWritable requesterid,
                    IntWritable nrequesters,
                    Context context)
           throws IOException, InterruptedException{
      //ConfigUtil.setupLogging(props, null);
      
      LinkStore linkstore = initStore(
          ConfigUtil.getPropertyRequired(props, Config.LINKSTORE_CLASS),
          Phase.REQUEST, requesterid.get());

      String nodeStoreClassName = props.getProperty(Config.NODESTORE_CLASS);
      NodeStore nodestore = createNodeStore(nodeStoreClassName, linkstore);
      LatencyStats latencyStats = new LatencyStats(nrequesters.get());
      RequestProgress progress =
                              LinkBenchRequest.createProgress(logger, props);
      progress.startTimer();
      // TODO: Don't support NodeStore yet
      final LinkBenchRequest requester =
        new LinkBenchRequest(linkstore, nodestore, props, latencyStats, null, progress,
                new Random(), requesterid.get(), nrequesters.get());

      requester.run();
      long requestdone = requester.getRequestsDone();

      context.write(new IntWritable(REQUESTMAPPER),
                     new LongWritable(requestdone));
      if (REPORT_PROGRESS) {
        context.getCounter(Counters.REQUEST_DONE).increment(requestdone);
      }
    }
  }

  /**
   * Reducer for both LOAD and REQUEST
   * Get the sum of "loaded links" or "finished requests"
   */
  public static class LoadRequestReducer
    extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
    private long sum[] = new long[3];

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      sum[NODEMAPPER] = 0;
      sum[LINKMAPPER] = 0;
    }

    // FIXME : need to differentiate node and link
    @Override
    public void reduce(IntWritable mapper_type,
                       Iterable<LongWritable> values,
                       Context context)
           throws IOException, InterruptedException {

      int type = mapper_type.get();
      for(LongWritable value : values) {
        sum[type] += value.get();
      }
      context.write(mapper_type, new LongWritable(sum[type]));
    }

    /**
     * Reduce task done, write output to a file.
     */
    @Override
    protected void cleanup(Context context) throws IOException {
      //write output to a file
      Path outDir = new Path(TMP_DIR, "out");
      Path outFile = new Path(outDir, "reduce-out");
      FileSystem fileSys = FileSystem.get(context.getConfiguration());
      SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, context.getConfiguration(),
          outFile, IntWritable.class, LongWritable.class,
          CompressionType.NONE);
      writer.append(new IntWritable(NODEMAPPER), new LongWritable(sum[NODEMAPPER]));
      writer.append(new IntWritable(LINKMAPPER), new LongWritable(sum[LINKMAPPER]));
      writer.close();
    }
  }

  /**
   * main route of the LOAD phase
   * @throws ClassNotFoundException 
   */
  private void load() throws IOException, InterruptedException, ClassNotFoundException {
    boolean loaddata = (props.containsKey(Config.LOAD_DATA)) &&
                        ConfigUtil.getBool(props, Config.LOAD_DATA);
    if (!loaddata) {
      logger.info("Skipping load data per the config");
      return;
    }

    int nlinkloaders = ConfigUtil.getInt(props, Config.NUM_LOADERS);
    int nnodeloaders = ConfigUtil.getInt(props, Config.NUM_NODE_LOADERS);
    Job loadjob = prepareJob(LOAD, nlinkloaders, nnodeloaders);
    FileSystem fs = setupInputFiles(loadjob, LOAD, nlinkloaders, nnodeloaders);

    try {
      logger.info("Starting loaders " + nlinkloaders);
      final long starttime = System.currentTimeMillis();
      loadjob.waitForCompletion(true);

      long loadtime = (System.currentTimeMillis() - starttime);

      // compute total #links loaded
      long maxid1 = ConfigUtil.getLong(props, Config.MAX_ID);
      long startid1 = ConfigUtil.getLong(props, Config.MIN_ID);
      long counts[] = readOutput(LOAD, fs, loadjob);

      logger.info("LOAD PHASE COMPLETED. "
          + "ID ranage : " + startid1 + " - " + maxid1 + ".");

      logger.info("Actually " + counts[NODEMAPPER] + " nodes loaded, "
          + counts[LINKMAPPER] + " links loaded in " + (loadtime / 1000) + " seconds, "
          + "Links/second = " + ((1000 * counts[LINKMAPPER]) / loadtime));
    } finally {
      logger.info("Deleting " + TMP_DIR);
      fs.delete(TMP_DIR, true);
    }
  }

  /**
   * main route of the REQUEST phase
   * @throws ClassNotFoundException 
   */
  private void sendrequests() throws IOException, InterruptedException, ClassNotFoundException {
    // config info for requests
    
    boolean dorequest = (props.containsKey(Config.DO_REQUEST))
        && ConfigUtil.getBool(props, Config.DO_REQUEST);
    if (!dorequest) {
      logger.info("Skipping request data per the config");
      return;
    }
    
    int nrequesters = ConfigUtil.getInt(props, Config.NUM_REQUESTERS);
    Job requestJob = prepareJob(REQUEST, nrequesters, 0);
    FileSystem fs = setupInputFiles(requestJob, REQUEST, nrequesters, 0);

    try {
      logger.info("Starting requesters " + nrequesters);
      final long starttime = System.currentTimeMillis();
      requestJob.waitForCompletion(true);
      long endtime = System.currentTimeMillis();

      // request time in millis
      long requesttime = (endtime - starttime);
      long counts[] = readOutput(REQUEST, fs, requestJob);

      logger.info("REQUEST PHASE COMPLETED. " + counts[REQUESTMAPPER] +
                         " requests done in " + (requesttime/1000) + " seconds." +
                         "Requests/second = " + (1000*counts[REQUESTMAPPER])/requesttime);
    } finally {
      fs.delete(TMP_DIR, true);
    }
  }

  
  /**
   * read in configuration and invoke LOAD and REQUEST
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Args : LinkBenchDriver configfile");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    props = ConfigUtil.parseConfigFile(args[0], configFileNames);
    logger.info("configFile name = " + configFileNames[ConfigUtil.CONFIGFILEPOS]);
    logger.info("workloadConfigFile name = " + configFileNames[ConfigUtil.WORKLOADFILEPOS]);
    logger.info("distributionDataFile name = " + configFileNames[ConfigUtil.DISTFILEPOS]);

    // get name or temporary directory
    String tempdirname = props.getProperty(Config.TEMPDIR);
    if (tempdirname != null) {
      TMP_DIR = new Path(tempdirname);
    }
    // whether report progress through reporter
    REPORT_PROGRESS = (!props.containsKey(Config.MAPRED_REPORT_PROGRESS)) ||
        ConfigUtil.getBool(props, Config.MAPRED_REPORT_PROGRESS);

    String files = getConf().get("tmpfiles");
    logger.info("tmpfiles = " + files);

    load();
    sendrequests();
    return 0;
  }

  /**
   * Load a class by name.
   * @param name the class name.
   * @return the class object.
   * @throws ClassNotFoundException if the class is not found.
   */
  public static Class<?> getClassByName(String name)
    throws ClassNotFoundException {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return Class.forName(name, true, classLoader);
  }

  /** Create an object for the given class and initialize it from conf
   *
   * @param theClass class of which an object is created
   * @param conf Configuration
   * @return a new object
   */
  public static <T> T newInstance(Class<T> theClass) {
    T result;
    try {
      Constructor<T> meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
      meth.setAccessible(true);
      result = meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(null, new LinkBenchDriverMR(), args));
  }
}


