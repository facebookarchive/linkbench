package com.facebook.LinkBench;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.facebook.LinkBench.LinkBenchLoad.LoadProgress;

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
  private static Path TMP_DIR = new Path("TMP_Link_Bench");
  private static boolean REPORT_PROGRESS = false;
  private static boolean USE_INPUT_FILES = false; //use generate input by default

  private static final Logger logger =
              Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
  
  static enum Counters { LINK_LOADED, REQUEST_DONE }

  private static Properties props;
  private static String store;

  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};

  /**
   * generate an instance of LinkStore
   * @param currentphase LOAD or REQUEST
   * @param mapperid id of the mapper 0, 1, ...
   */
  private static LinkStore initStore(int currentphase, int mapperid)
    throws IOException {

    LinkStore newstore = null;

    if (store == null) {
      store = props.getProperty("store");
      logger.info("Using store class: " + store);
    }

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
      clazz = getClassByName(store);
    } catch (java.lang.ClassNotFoundException nfe) {
      throw new IOException("Cound not find class for " + store);
    }
    newstore = (LinkStore)newInstance(clazz);
    if (clazz == null) {
      throw new IOException("Unknown data store " + store);
    }
    
    try { 
      newstore.initialize(props, currentphase, mapperid);
    } catch (Exception e) {
      throw new IOException(e);
    }
    return newstore;
  }

  /**
   * InputSplit for generated inputs
   */
  private class LinkBenchInputSplit implements InputSplit {
    private int id;  // id of mapper
    private int num; // total number of mappers

    LinkBenchInputSplit() {}
    public LinkBenchInputSplit(int i, int n) {
      this.id = i;
      this.num = n;
    }
    public int getID() {return this.id;}
    public int getNum() {return this.num;}
    public long getLength() {return 1;}

    public String[] getLocations() throws IOException {
      return new String[]{};
    }

    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.num = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeInt(this.num);
    }

  }

  /**
   * RecordReader for generated inputs
   */
  private class LinkBenchRecordReader
    implements RecordReader<IntWritable, IntWritable> {
    private int id;
    private int num;
    private boolean done;

    public LinkBenchRecordReader(LinkBenchInputSplit split) {
      this.id = split.getID();
      this.num = split.getNum();
      this.done = false;
    }

    public IntWritable createKey() {return new IntWritable();}
    public IntWritable createValue() {return new IntWritable();}
    public void close() throws IOException { }

    // one loader per split
    public float getProgress() { return 0.5f;}
    // one loader per split
    public long getPos() {return 1;}

    public boolean next(IntWritable key, IntWritable value)
      throws IOException {
      if (this.done) {
        return false;
      } else {
        key.set(this.id);
        value.set(this.num);
        this.done = true;
      }
      return true;
    }
  }

  /**
   * InputFormat for generated inputs
   */
  private class LinkBenchInputFormat
    implements InputFormat<IntWritable, IntWritable> {

    public InputSplit[] getSplits(JobConf conf, int numsplits) {
      InputSplit[] splits = new InputSplit[numsplits];
      for (int i = 0; i < numsplits; ++i) {
        splits[i] = (InputSplit) new LinkBenchInputSplit(i, numsplits);
      }
      return splits;
    }

    public RecordReader<IntWritable, IntWritable> getRecordReader(
      InputSplit split, JobConf conf, Reporter reporter) {
      return (RecordReader)(new LinkBenchRecordReader((LinkBenchInputSplit)split));
    }

    public void validateInput(JobConf conf) {}  // no need to validate
  }

  /**
   * create JobConf for map reduce job
   * @param currentphase LOAD or REQUEST
   * @param nmappers number of mappers (loader or requester)
   */
  private JobConf createJobConf(int currentphase, int nmappers) {
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
    jobconf.setNumMapTasks(nmappers);
    jobconf.setReducerClass(LoadRequestReducer.class);
    jobconf.setNumReduceTasks(1);

    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    jobconf.setSpeculativeExecution(false);

    return jobconf;
  }

  /**
   * setup input files for map reduce job
   * @param jobconf configuration of the map reduce job
   * @param nmappers number of mappers (loader or requester)
   */
  private static FileSystem setupInputFiles(JobConf jobconf, int nmappers)
    throws IOException, InterruptedException {
    //setup input/output directories
    final Path indir = new Path(TMP_DIR, "in");
    final Path outdir = new Path(TMP_DIR, "out");
    FileInputFormat.setInputPaths(jobconf, indir);
    FileOutputFormat.setOutputPath(jobconf, outdir);

    final FileSystem fs = FileSystem.get(jobconf);
    if (fs.exists(TMP_DIR)) {
      throw new IOException("Tmp directory " + fs.makeQualified(TMP_DIR)
          + " already exists.  Please remove it first.");
    }
    if (!fs.mkdirs(indir)) {
      throw new IOException("Cannot create input directory " + indir);
    }

    //generate an input file for each map task
    if (USE_INPUT_FILES) {
      for(int i=0; i < nmappers; ++i) {
        final Path file = new Path(indir, "part"+i);
        final IntWritable mapperid = new IntWritable(i);
        final IntWritable nummappers = new IntWritable(nmappers);
        final SequenceFile.Writer writer = SequenceFile.createWriter(
          fs, jobconf, file,
          IntWritable.class, IntWritable.class, CompressionType.NONE);
        try {
          writer.append(mapperid, nummappers);
        } finally {
          writer.close();
        }
        logger.info("Wrote input for Map #"+i);
      }
    }
    return fs;
  }

  /**
   * read output from the map reduce job
   * @param fs the DFS FileSystem
   * @param jobconf configuration of the map reduce job
   */
  public static long readOutput(FileSystem fs, JobConf jobconf)
    throws IOException, InterruptedException {
    //read outputs
    final Path outdir = new Path(TMP_DIR, "out");
    Path infile = new Path(outdir, "reduce-out");
    IntWritable nworkers = new IntWritable();
    LongWritable result = new LongWritable();
    long output = 0;
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, infile, jobconf);
    try {
      reader.next(nworkers, result);
      output = result.get();
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
  public static class LoadMapper extends MapReduceBase
    implements Mapper<IntWritable, IntWritable, IntWritable, LongWritable> {

    public void map(IntWritable loaderid,
                    IntWritable nloaders,
                    OutputCollector<IntWritable, LongWritable> output,
                    Reporter reporter) throws IOException {
      ConfigUtil.setupLogging(props, null);
      LinkStore store = initStore(LOAD, loaderid.get());
      LinkBenchLatency latencyStats = new LinkBenchLatency(nloaders.get());

      long maxid1 = Long.parseLong(props.getProperty("maxid1"));
      long startid1 = Long.parseLong(props.getProperty("startid1"));
      
      LoadProgress prog_tracker = new LoadProgress(
          Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER), maxid1 - startid1);
      
      LinkBenchLoad loader = new LinkBenchLoad(store, props, latencyStats,
                               loaderid.get(), maxid1 == startid1 + 1,
                               nloaders.get(), prog_tracker);
      
      LinkedList<LinkBenchLoad> tasks = new LinkedList<LinkBenchLoad>();
      tasks.add(loader);
      long linksloaded = 0;
      try {
        LinkBenchDriver.concurrentExec(tasks);
        linksloaded = loader.getLinksLoaded();
      } catch (java.lang.Throwable t) {
        throw new IOException(t);
      }
      output.collect(new IntWritable(nloaders.get()),
                     new LongWritable(linksloaded));
      if (REPORT_PROGRESS) {
        reporter.incrCounter(Counters.LINK_LOADED, linksloaded);
      }
    }
  }

  /**
   * Mapper for REQUEST phase
   * Send requests
   * Output the number of finished requests
   */
  public static class RequestMapper extends MapReduceBase
    implements Mapper<IntWritable, IntWritable, IntWritable, LongWritable> {

    public void map(IntWritable requesterid,
                    IntWritable nrequesters,
                    OutputCollector<IntWritable, LongWritable> output,
                    Reporter reporter) throws IOException {
      ConfigUtil.setupLogging(props, null);
      LinkStore store = initStore(REQUEST, requesterid.get());
      LinkBenchLatency latencyStats = new LinkBenchLatency(nrequesters.get());
      final LinkBenchRequest requester =
        new LinkBenchRequest(store, props, latencyStats,
                             requesterid.get(), nrequesters.get());
      
      // Wrap in runnable to handle error
      Thread t = new Thread(new Runnable() {
        public void run() {
          try {
            requester.run();
          } catch (Throwable t) {
            logger.error("Uncaught error in requester:", t);
          }
        }
      });
      t.start();
      long requestdone = 0;
      try {
        t.join();
        requestdone = requester.getRequestsDone();
      } catch (InterruptedException e) {
      }
      output.collect(new IntWritable(nrequesters.get()),
                     new LongWritable(requestdone));
      if (REPORT_PROGRESS) {
        reporter.incrCounter(Counters.REQUEST_DONE, requestdone);
      }
    }
  }

  /**
   * Reducer for both LOAD and REQUEST
   * Get the sum of "loaded links" or "finished requests"
   */
  public static class LoadRequestReducer extends MapReduceBase
    implements Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
    private long sum = 0;
    private int nummappers = 0;
    private JobConf conf;

    /** Store job configuration. */
    @Override
    public void configure(JobConf job) {
      conf = job;
    }

    public void reduce(IntWritable nmappers,
                       Iterator<LongWritable> values,
                       OutputCollector<IntWritable, LongWritable> output,
                       Reporter reporter) throws IOException {

      nummappers = nmappers.get();
      while(values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(new IntWritable(nmappers.get()),
                     new LongWritable(sum));
    }

    /**
     * Reduce task done, write output to a file.
     */
    @Override
    public void close() throws IOException {
      //write output to a file
      Path outDir = new Path(TMP_DIR, "out");
      Path outFile = new Path(outDir, "reduce-out");
      FileSystem fileSys = FileSystem.get(conf);
      SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
          outFile, IntWritable.class, LongWritable.class,
          CompressionType.NONE);
      writer.append(new IntWritable(nummappers), new LongWritable(sum));
      writer.close();
    }
  }

  /**
   * main route of the LOAD phase
   */
  private void load() throws IOException, InterruptedException {
    boolean loaddata = Boolean.parseBoolean(props.getProperty("loaddata"));
    if (!loaddata) {
      logger.info("Skipping load data per the config");
      return;
    }

    int nloaders = Integer.parseInt(props.getProperty("loaders"));
    final JobConf jobconf = createJobConf(LOAD, nloaders);
    FileSystem fs = setupInputFiles(jobconf, nloaders);

    try {
      logger.info("Starting loaders " + nloaders);
      final long starttime = System.currentTimeMillis();
      JobClient.runJob(jobconf);
      long loadtime = (System.currentTimeMillis() - starttime);

      // compute total #links loaded
      long maxid1 = Long.parseLong(props.getProperty("maxid1"));
      long startid1 = Long.parseLong(props.getProperty("startid1"));
      int nlinks_default = Integer.parseInt(props.getProperty("nlinks_default"));
      long expectedlinks = (1 + nlinks_default) * (maxid1 - startid1);
      long actuallinks = readOutput(fs, jobconf);

      logger.info("LOAD PHASE COMPLETED. Expected to load " +
                         expectedlinks + " links. " +
                         actuallinks + " loaded in " + (loadtime/1000) + " seconds." +
                         "Links/second = " + ((1000*actuallinks)/loadtime));
    } finally {
      fs.delete(TMP_DIR, true);
    }
  }

  /**
   * main route of the REQUEST phase
   */
  private void sendrequests() throws IOException, InterruptedException {
    // config info for requests
    int nrequesters = Integer.parseInt(props.getProperty("requesters"));
    final JobConf jobconf = createJobConf(REQUEST, nrequesters);
    FileSystem fs = setupInputFiles(jobconf, nrequesters);

    try {
      logger.info("Starting requesters " + nrequesters);
      final long starttime = System.currentTimeMillis();
      JobClient.runJob(jobconf);
      long endtime = System.currentTimeMillis();

      // request time in millis
      long requesttime = (endtime - starttime);
      long requestsdone = readOutput(fs, jobconf);

      logger.info("REQUEST PHASE COMPLETED. " + requestsdone +
                         " requests done in " + (requesttime/1000) + " seconds." +
                         "Requests/second = " + (1000*requestsdone)/requesttime);
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
    props = new Properties();
    props.load(new FileInputStream(args[0]));

    // get name or temporary directory
    String tempdirname = props.getProperty("tempdir");
    if (tempdirname != null) {
      TMP_DIR = new Path(tempdirname);
    }
    // whether report progress through reporter
    REPORT_PROGRESS = Boolean.parseBoolean(props.getProperty("reportprogress"));
    // whether store mapper input in files
    USE_INPUT_FILES = Boolean.parseBoolean(props.getProperty("useinputfiles"));

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


