package com.facebook.LinkBench;

import java.io.IOException;
import java.util.Properties;

public class MemoryLinkStoreTest extends LinkStoreTestBase {
  
  MemoryLinkStore store;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @Override
  protected Properties basicProps() {
    Properties props = super.basicProps();
    props.setProperty(Config.LINKSTORE_CLASS, MemoryLinkStore.class.getName());
    return props;
  }
  
  @Override
  protected void initStore(Properties props) throws IOException,
      Exception {
    store = new MemoryLinkStore();
  }

  @Override
  protected DummyLinkStore getStoreHandle(boolean initialized) {
    // Return a new memory link store handle. The underlying link store doesn't need to be 
    // initialized, so just set wrapper to correct init status
    return new DummyLinkStore(store.newHandle(), initialized);
  }

}
