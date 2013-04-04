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

public class LinkCount {

  public final long id1;
  public final long link_type;
  public long time;
  public long version;
  public long count;
  public LinkCount(long id1, long link_type,
      long time, long version, long init_count) {
    super();
    this.id1 = id1;
    this.link_type = link_type;
    this.time = time;
    this.version = version;
    this.count = init_count;
  }
}
