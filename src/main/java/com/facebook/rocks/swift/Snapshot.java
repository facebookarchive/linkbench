package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

@ThriftStruct("Snapshot")
public class Snapshot
{
private long snapshotid;

@ThriftField(value=1, name="snapshotid")
public long getSnapshotid() { return snapshotid; }

@ThriftField(value=1, name="snapshotid")
public void setSnapshotid(final long snapshotid) { this.snapshotid = snapshotid; }
}
