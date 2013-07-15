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
import java.util.Properties;

public class MemoryGraphStoreTest extends GraphStoreTestBase {

  MemoryLinkStore store;
  @Override
  protected void initStore(Properties props) throws IOException, Exception {
    store = new MemoryLinkStore();
  }

  @Override
  protected StatsDummyLinkStore getStoreHandle(boolean initialized) throws IOException, Exception {
    return new StatsDummyLinkStore(store.newHandle(), initialized);
  }

}
