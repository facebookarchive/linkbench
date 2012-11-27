package com.facebook.LinkBench;

import java.util.Arrays;


public class Link {

  public Link(long id1, long link_type, long id2,
      byte visibility, byte[] data, int version, long time) {
    this.id1 = id1;
    this.link_type = link_type;
    this.id2 = id2;
    this.visibility = visibility;
    this.data = data;
    this.version = version;
    this.time = time;
  }

  Link() {
    link_type = LinkStore.DEFAULT_LINK_TYPE;
    visibility = LinkStore.VISIBILITY_DEFAULT;
  }

  public boolean equals(Object other) {
    if (other instanceof Link) {
      Link o = (Link) other;
      return id1 == o.id1 && id2 == o.id2 &&
          link_type == o.link_type &&
          visibility == o.visibility &&
          version == o.version && time == o.time &&
          Arrays.equals(data, o.data);
    } else {
      return false;
    }
  }
  
  public String toString() {
    return String.format("Link(id1=%d, id2=%d, link_type=%d," +
    		"visibility=%d, version=%d," +
    		"time=%d, data=%s", id1, id2, link_type,
    		visibility, version, time, data.toString());
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
    l.visibility = this.visibility;
    l.data = this.data.clone();
    l.version = this.version;
    l.time = this.time;
    return l;
  }

  public long id1;
  public long link_type;
  public long id2;
  public byte visibility;
  public byte[] data;
  public int version;
  public long time;

}
