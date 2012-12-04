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

// Various operation types for which we want to gather stats
public enum LinkBenchOp {
  ADD_NODE,
  UPDATE_NODE,
  DELETE_NODE,
  GET_NODE,
  ADD_LINK,
  DELETE_LINK,
  UPDATE_LINK,
  COUNT_LINK,
  MULTIGET_LINK,
  GET_LINKS_LIST,
  LOAD_NODE_BULK,
  LOAD_LINK,
  LOAD_LINKS_BULK,
  LOAD_COUNTS_BULK,
  // Although the following are not truly operations, we need stats
  // for them 
  RANGE_SIZE,    // how big range scans are
  LOAD_LINKS_BULK_NLINKS, // how many links inserted in bulk
  LOAD_COUNTS_BULK_NLINKS, // how many counts inserted in bulk
  UNKNOWN;
  
  public String displayName() {
    return name();
  }
}