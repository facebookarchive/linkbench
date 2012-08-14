package com.facebook.LinkBench;

public class LinkCount {

  public final long id1;
  public final int id1_type;
  public final long link_type;
  public long time;
  public long version;
  public long count;
  public LinkCount(long id1, int id1_type, long link_type, 
      long time, long version, long init_count) {
    super();
    this.id1 = id1;

    this.id1_type = id1_type;
    this.link_type = link_type;
    this.time = time;
    this.version = version;
    this.count = init_count;
  }
}
