package com.facebook.LinkBench;

import java.io.IOException;
import java.util.Properties;

public class DummyLinkStoreTest extends LinkStoreTestBase {

  private Properties props;

  @Override
  protected void initStore(Properties props) {
    // Do nothing
    this.props = props;
  }

  @Override
  protected DummyLinkStore getStoreHandle() throws IOException, Exception {
    DummyLinkStore store = new DummyLinkStore();
    store.initialize(props, Phase.REQUEST, 0);
    return store;
  }
}
