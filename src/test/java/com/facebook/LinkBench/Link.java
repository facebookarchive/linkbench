package com.facebook.LinkBench;


public class Link {

  Link() {
    link_type = LinkStore.LINK_TYPE;
    id1_type = LinkStore.ID1_TYPE;
    id2_type = LinkStore.ID2_TYPE;
    visibility = LinkStore.VISIBILITY_DEFAULT;
  }

  public long id1;
  public long link_type;
  public long id2;
  public int id1_type;
  public int id2_type;
  public byte visibility;
  public byte[] data;
  public int version;
  public long time;

}
