package com.facebook.LinkBench;

/**
 * The same as runnable, except run() can throw Exceptions
 * to be handled by the caller.
 */
public interface LinkBenchTask {
  public abstract void run() throws Exception;
}
