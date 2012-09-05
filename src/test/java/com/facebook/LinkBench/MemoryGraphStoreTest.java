package com.facebook.LinkBench;

import java.io.IOException;
import java.util.Properties;

public class MemoryGraphStoreTest extends GraphStoreTestBase {

  MemoryLinkStore store;
  @Override
  protected void initStore(Properties props) throws IOException, Exception {
    store = new MemoryLinkStore();
  }

  @Override
  protected DummyLinkStore getStoreHandle(boolean initialized) throws IOException, Exception {
    return new DummyLinkStore(store.newHandle(), initialized);
  }

}
