package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("Snapshot")
public class Snapshot
{
    @ThriftConstructor
    public Snapshot(
        @ThriftField(value=1, name="snapshotid") final long snapshotid
    ) {
        this.snapshotid = snapshotid;
    }

    private final long snapshotid;

    @ThriftField(value=1, name="snapshotid")
    public long getSnapshotid() { return snapshotid; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("snapshotid", snapshotid)
            .toString();
    }
}
