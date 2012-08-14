package com.facebook.LinkBench;


public class Link {

  Link() {
    link_type = LinkStore.LINK_TYPE;
    id1_type = LinkStore.ID1_TYPE;
    id2_type = LinkStore.ID2_TYPE;
    visibility = LinkStore.VISIBILITY_DEFAULT;
  }

  /**
   * Clone an existing link
   * @param l
   */
  public Link clone() {
    Link l = new Link();
    l.id1 = this.id1;
    l.link_type = this.link_type;
    l.id2 = this.id2;
    l.id1_type = this.id1_type;
    l.id2_type = this.id2_type;
    l.visibility = this.visibility;
    l.data = this.data.clone();
    l.version = this.version;
    l.time = this.time;
    return l;
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
