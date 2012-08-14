package com.facebook.LinkBench;

import java.io.IOException;
import java.util.Properties;

import com.facebook.LinkBench.LinkStore;
import com.facebook.LinkBench.MemoryLinkStore;

public class MemoryLinkStoreTest extends LinkStoreTestBase {

  @Override
  protected LinkStore createStore(Properties props) throws IOException,
      Exception {
    return new MemoryLinkStore();
  }

}
