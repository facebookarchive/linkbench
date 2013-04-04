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

import java.util.Arrays;

/**
 * Object node in social graph
 * @author tarmstrong
 */
public class Node {
  public long id;
  public int type;
  public long version;
  public int time;
  public byte data[];

  public Node(long id, int type, long version, int time,
      byte data[]) {
    super();
    this.id = id;
    this.type = type;
    this.version = version;
    this.time = time;
    this.data = data;
  }

  public Node clone() {
    return new Node(id, type, version, time, data);
  }
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Node)) {
      return false;
    }
    Node o = (Node) other;
    return id == o.id && type == o.type && version == o.version
        && time == o.time && Arrays.equals(data, o.data);
  }

  public String toString() {
    return "Node(" + "id=" + id + ",type=" + type + ",version=" + version + ","
                   + "timestamp=" + time + ",data="
                   + Arrays.toString(data) + ")";
  }
}
