package com.facebook.LinkBench;

import org.junit.Test;

import com.facebook.LinkBench.distributions.Harmonic;

import junit.framework.TestCase;

public class HarmonicTest extends TestCase {

  @Test
  public void testHarmonic() {
    assertEquals(1, Harmonic.generalizedHarmonic(1, 0.8), 0.001);
    assertEquals(1.99534, Harmonic.generalizedHarmonic(10, 1.5), 0.00001);
    assertEquals(61.8010, Harmonic.generalizedHarmonic(1000, 0.5), 0.001);
    assertEquals(207.541, Harmonic.generalizedHarmonic(1000000, 0.7), 0.002);
    assertEquals(2679914.0, Harmonic.generalizedHarmonic(12345678, 0.1),
                 1);
  }
  
  @Test
  public void testApprox() {
    
    // Test that approximation is close to actual for a range of shapes and 
    // ns
    double shapes[] = {0.01, 0.1, 0.5, 0.9, 0.99};
    
    for (long i = 0; i < 30; i+=4) {
      long n = (long)Math.pow(2, i);
      for (double shape: shapes) {
        double exact = Harmonic.generalizedHarmonic(n, shape);
        long start = System.currentTimeMillis();
        double approx = Harmonic.generalizedHarmonicApprox(n, shape);
        long end = System.currentTimeMillis();
        System.err.format("generalizedHarmonicApprox(%d, %f) took %.3fs\n", n, shape, (end - start) / 1000.0);
        double err = approx - exact;
        double errPc = (err / exact) * 100.0;
        System.err.format("generalizedHarmonicApprox(%d, %f) = %f.  exact=%f err=%f err%%=%.2f\n", n,
            shape, approx, exact, err, errPc);
        double errThresh = 0.05;
        assertTrue(String.format("Err%%=%.3f must be < 0.05%%", Math.abs(errPc)),
                                 Math.abs(errPc) < errThresh);
      }
    }
  }
}
