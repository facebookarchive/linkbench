package com.facebook.LinkBench;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.log4j.Logger;

/*
 * This class simulates the real distribution based on statistical data.
 */

class RealDistribution {
  private static final Logger logger = 
                      Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER); 
  /* params to shuffle
  final static long[] NLINKS_SHUFFLER_PARAMS = {13, 7};
  final static long[] WRITE_SHUFFLER_PARAMS = {23, 13};
  final static long[] READ_SHUFFLER_PARAMS = {19, 11};
  */
  final static long[] NLINKS_SHUFFLER_PARAMS = {13, 7};
  final static long[] WRITE_SHUFFLER_PARAMS = {13, 7};
  final static long[] READ_SHUFFLER_PARAMS = {13, 7};

  //helper class to store (value, probability)
  static class Point implements Comparable {
    int value;
    double probability;
    Point(int input_value, double input_probability) {
      this.value = input_value;
      this.probability = input_probability;
    }
    public int compareTo(Object obj) {
      Point p = (Point)obj;
      return this.value - p.value;
    }
    public String toString() {
      return "(" + value + ", " + probability + ")";
    }
  };

  private static ArrayList<Point> nlinks_cdf, nreads_cdf, nwrites_cdf;
  private static double[] nreads_cs, nwrites_cs;
  private static long[] nreads_right_points, nwrites_right_points;
  static Random random_generator;

  /*
   * This method loads data from data file into memory;
   * must be called before any getNlinks or getNextId1s;
   * must be declared as synchronized method to prevent race condition.
   */
  static synchronized void loadOneShot(Properties props) throws Exception {
    if (nlinks_cdf == null) {
      logger.info("Loading real distribution data...");
      reload(props);
    }
  }

  static synchronized void reload(Properties props) throws Exception {
    getStatisticalData(props);
    random_generator = new Random();
  }


  //helper function: Calculate pdf from cdf
  private static double[] getPDF(ArrayList<Point> cdf) {
    int max_value = cdf.get(cdf.size() - 1).value;
    double[] pdf = new double[max_value + 1];

    // set all 0
    for (int i = 0; i < pdf.length; ++i) pdf[i] = 0;

    // convert cdf to pdf
    pdf[cdf.get(0).value] = cdf.get(0).probability;
    for (int i = 1; i < cdf.size(); ++i) {
      pdf[cdf.get(i).value] = cdf.get(i).probability -
        cdf.get(i - 1).probability;
    }
    return pdf;
  }

  //helper function: Calculate complementary cumulative distribution
  //function from pdf
  private static double[] getCCDF(double[] pdf) {
    int length = pdf.length;
    double[] ccdf = new double[length];
    ccdf[length - 1] = pdf[length - 1];
    for (int i = length - 2; i >= 0; --i) {
      ccdf[i] = ccdf[i + 1] + pdf[i];
    }
    return ccdf;
  }

  //helper function: Calculate cumulative sum
  private static double[] getCumulativeSum(double[] cdf) {
    int length = cdf.length;
    double[] cs = new double[length];
    cs[0] = 0; //ignore cdf[0]
    for (int i = 1; i < length; ++i) {
      cs[i] = cs[i - 1] + cdf[i];
    }
    return cs;
  }

  /*
   * This method get the area below the distribution nreads_ccdf or
   * nwrite_ccdf. This helps to determine the number of nreads after which
   * the generating distribution would be approximately equal to real
   * distribution.
   *
   * Keep in mind the because the number of id1s is constant, the
   * generating #reads distribution keeps changing. It starts at "100% 0",
   * keeps growing and eventually at some point (after certain number of
   * reads) it should be equal to the real #reads distribution.
   *
   * Because the number of id1s is constant (equal to maxid1 - startid1),
   * the total number of reads is also a constant, according to the
   * following fomular:
   *
   * (number of reads) = (number of id1s) x (area below nreads_pdf)
   *
   * To illustrate, consider the following nreads_pdf distribution:
   * 60%=0; 20%=1; 10%=2; 10%=3; and there are 100 id1s.
   *
   * The number of reads would be a constanst:
   * 100 * (20% * 1 + 10% * 2 + 10% * 3) = 100 * 80%.
   * The multiplication factor (20% * 1 + 10% * 2 + 10% * 3) is what we
   * want this method to return.
   *
   * If we already have the ccdf (comlementary cumulative distribution
   * function): 40%>=1; 20%>=2; 10%>=3; and its cumulative sum:
   * [40%, 40%+20%, 40%+20%+10%] = [40%, 60%, 80%], then just need to
   * return the last cumulative sum (80%).
   */
  static double getArea(String type) {
    if (type.equals("nreads")) return nreads_cs[nreads_cs.length - 1];
    else if (type.equals("nwrites")) return nwrites_cs[nwrites_cs.length - 1];
    else return 0;
  }


  //helper function:
  private static ArrayList<Point> readCDF(Scanner scanner) {
    ArrayList<Point> points = new ArrayList<Point>();
    while (scanner.hasNextInt()) {
      int value = scanner.nextInt();
      double probability = scanner.nextDouble();
      Point temp = new Point(value, probability);
      points.add(temp);
    }
    return points;
  }

  //convert CDF from ArrayList<Point> to Map
  static Map getCDF(String name) {
    ArrayList<Point> points = name.equals("nlinks") ? nlinks_cdf :
      name.equals("nreads") ? nreads_cdf :
      name.equals("nwrites") ? nwrites_cdf : null;
    if (points == null) return null;

    Map<Integer, Double> map = new TreeMap<Integer, Double>();
    for (Point point : points) {
      map.put(point.value, point.probability);
    }
    return map;
  }

  //for debugging purpose
  private static void print(double[] a) {
    for (int i=0; i < a.length; ++i) System.out.print(a[i] + " ");
    System.out.println();
  }

  /*
   * This method reads from data_file nlinks, nreads, nwrites discreate
   * cumulative distribution function (CDF) and produces corresponding
   * pdf and ccdf.
   *
   * The data file is generated by LinkBenchConfigGenerator, and can be
   * located by parameter data_file in the config file.
   *
   * CDF is returned under the form of an array whose elements are pairs of
   * value and the cumulative distribution at that value i.e. <x, CDF(x)>.
   */
  private static void getStatisticalData(Properties props) throws Exception {
    String propName = "data_file";
    String filename = props.getProperty(propName);
    
    // If relative path, should be relative to linkbench home directory
    String fileAbsPath;
    if (new File(filename).isAbsolute()) {
      fileAbsPath = filename;
    } else {
      String linkBenchHome = ConfigUtil.findLinkBenchHome();
      if (linkBenchHome == null) {
        throw new Exception("Data file config property " + propName
            + " was specified using a relative path, but linkbench home"
            + " directory was not specified through environment var "
            + ConfigUtil.linkbenchHomeEnvVar);
      } else {
        fileAbsPath = linkBenchHome + File.separator + filename;
      }
    }
    
    Scanner scanner = new Scanner(new File(fileAbsPath));
    while (scanner.hasNext()) {
      String type = scanner.next();
      if (type.equals("nlinks")) {
        nlinks_cdf = readCDF(scanner);
      }
      else if (type.equals("nreads")) {
        nreads_cdf = readCDF(scanner);
        double[] nreads_pdf = getPDF(nreads_cdf);
        double[] nreads_ccdf = getCCDF(nreads_pdf);
        nreads_cs = getCumulativeSum(nreads_ccdf);

        nreads_right_points = new long[nreads_cs.length];
        for (int i = 0; i < nreads_right_points.length; ++i) {
          nreads_right_points[i] = 0;
        }
      }
      else if (type.equals("nwrites")) {
        nwrites_cdf = readCDF(scanner);
        double[] nwrites_pdf = getPDF(nwrites_cdf);
        double[] nwrites_ccdf = getCCDF(nwrites_pdf);
        nwrites_cs = getCumulativeSum(nwrites_ccdf);

        nwrites_right_points = new long[nwrites_cs.length];
        for (int i = 0; i < nwrites_right_points.length; ++i) {
          nwrites_right_points[i] = 0;
        }
      }
      else {
        throw new Exception("Invalid file content: " + type);
      }
    }
  }

  /*
   * To estimate #nlinks of id1, we search for the smallest x and
   * largest y in the CDF data that satisfies
   * CDF(y) < CDF(id1/maxid1) <= CDF(x) and then return a random number
   * between (y+1) and x as #nlinks of id1.
   *
   * That is equivalent to searching for the idx that satisfies
   * data[idx - 1].probability < p <= data[idx].probability and then
   * return a random number between (data[idx - 1].value + 1) and
   * data[idx].value.
   *
   * Caution: getNlinks is meant to estimate #nlinks of id1. In the
   * function, we use random number. Therefore, even with the same
   * parameter, two calls of getNlinks can result in different results.
   *
   * Large ids tend to have (very) large #nlinks, so we swap 50% large ids
   * with small ids to make workload more balanced. This technique is not
   * very effective (only reduce around 30% running time in case of
   * assoc_type FRIEND_REQUESTS_SEND_RECEIVED_ASSOC), but for now it is
   * better than nothing.
   */

  // searching for smallest idx that p <= data[idx].probability
  static int binarySearch(ArrayList<Point> points, double p) {
    int idx = 0;
    int left = 0, right = points.size() - 1;
    while (left <= right) {
      int mid = (left + right)/2;
      if (points.get(mid).probability >= p) {
        idx = mid;
        right = mid - 1;
      }
      else {
        left = mid + 1;
      }
    }
    return idx;
  }

  //return a random value within a specific range (inclusive both ends)
  static long randomRange(int left, int right) {
    return left + random_generator.nextInt(right - left + 1);
  }

  static long shuffleAndGetNlinks(long id1_and_newid1[],
                                  long startid1, long maxid1) {
    long id1 = id1_and_newid1[0];

    long newid1 = Shuffler.getPermutationValue(id1, startid1, maxid1,
                                               NLINKS_SHUFFLER_PARAMS);

    id1_and_newid1[1] = newid1;
    return getNlinks(newid1, startid1, maxid1);
  }

  static long getNlinks(long id1, long startid1, long maxid1) {
    // simple workload balancing
    double p = (100.0 * (id1 - startid1 + 1)) / (maxid1 - startid1);
    int idx = binarySearch(nlinks_cdf, p);

    if (idx == 0) {
      return 0;
    }
    else  {
      return randomRange(nlinks_cdf.get(idx - 1).value + 1,
          nlinks_cdf.get(idx).value);
    }
  }

  /*
   * Like Random.nextInt() generates a sequence of random numbers,
   * this function generates a sequence of random id1s. The different
   * is taht it follows the distribution of nwrites_cdf or nreads_cdf
   * rather than uniform distribution.
   *
   * The algorithm is a bit tricky. For illustration, let's consider
   * the following pdf (where x stands for #writes or #reads):
   * p(x=0)=0.6; p(x=1)=0.2; p(x=3)=0.1; p(x=5)=0.1.
   *
   * Firstly, we take the complementary cumulative distribution (A):
   * p(x>=5)=0.1; p(x>=4)=0.1; p(x>=3)=0.2; p(x>=2)=0.2; p(x>=1)=0.4;
   * p(x>=0)=1;
   *
   * Say we have N id1s, this is how we want the final result to be:
   * id1s in [0->0.1*N) have x>=5
   * id1s in [0->0.1*N) have x>=4 (meaning there is no x=4)
   * id1s in [0->0.2*N) have x>=3 (meaning id1s in [0.1*N, 0.2*N) have x=3)
   * id1s in [0->0.2*N) have x>=2 (meaning there is no x=2)
   * id1s in [0->0.4*N) have x>=1 (meaning id1s in [0.2*N, 0.4*N) have x=1)
   * id1s in [0->N) have x>=0 (meaning id1s in [0.4*N, N) have x=0)
   *
   * To do this, we use an array "right" where right[i] means that id1s
   * in [0->right[i]] have x>=i. We initialize all right[i] (for all i)
   * value 0. In this example, right[i]=0 for i from 0 to 6. Our aim is
   * to eventually make right[5]=0.1*N; right[4]=0.1*N; right[3]=0.2*N;
   * right[2]=0.2*N; right[1]=0.4*N; and right[0]=N.
   *
   * Now every time getNextId1s is called, one right[i] will be randomly
   * picked according to distribution (A). Its value is returned by
   * getNextId1s as the new id1 before getting increased.
   *
   * The last question is how to randomly pick a right point according to
   * (A) effectively? In order to do this, we calculate the cumulative
   * sum of (A): 0, 0.4, 0.6, 0.8, 0.9, 1.0. To pick a right point, we
   * randomly generate a number from 0 to 1.0, and then use binary search
   * to find the corresonding range.
   */
  //find smallest idx that satisfy a[idx] >= p
  static int binarySearch(double[] a, double p) {
    int left = 0, right = a.length - 1;
    int idx = left;
    while (left <= right) {
      int mid = (left + right)/2;
      if (a[mid] >= p) {
        idx = mid;
        right = mid - 1;
      }
      else {
        left = mid + 1;
      }
    }
    return idx;
  }

  static long getNextId1(long startid1, long maxid1,
      boolean write) {

    double[] cs = write ? nwrites_cs : nreads_cs;//cumulative sum
    double max_probability = cs[cs.length - 1];
    double p = max_probability * Math.random();

    int idx = binarySearch(cs, p);
    if (idx == 0) idx = 1;

    long result = -1;
    if (write) {
      result = nwrites_right_points[idx];
      nwrites_right_points[idx] = (nwrites_right_points[idx] + 1)%maxid1;
    }
    else {
      result = nreads_right_points[idx];
      nreads_right_points[idx] = (nreads_right_points[idx] + 1)%maxid1;
    }

    long id1 = startid1 + result;

    if (write) {
      id1 = Shuffler.getPermutationValue(id1, startid1, maxid1,
          WRITE_SHUFFLER_PARAMS);
    }
    else {
      id1 = Shuffler.getPermutationValue(id1, startid1, maxid1,
          READ_SHUFFLER_PARAMS);
    }

    return id1;
  }
}

