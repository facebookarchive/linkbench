package com.facebook.LinkBench;

import java.util.Properties;

import com.facebook.LinkBench.LinkStore;

public class DummyLinkStoreTest extends LinkStoreTestBase {

  @Override
  protected LinkStore createStore(Properties props) {
    return null; // Test the dummy link store
  }
}
