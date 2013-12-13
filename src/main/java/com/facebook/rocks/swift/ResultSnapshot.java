package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("ResultSnapshot")
public class ResultSnapshot
{
    @ThriftConstructor
    public ResultSnapshot(
        @ThriftField(value=1, name="status") final RetCode status,
        @ThriftField(value=2, name="snapshot") final Snapshot snapshot
    ) {
        this.status = status;
        this.snapshot = snapshot;
    }

    private final RetCode status;

    @ThriftField(value=1, name="status")
    public RetCode getStatus() { return status; }

    private final Snapshot snapshot;

    @ThriftField(value=2, name="snapshot")
    public Snapshot getSnapshot() { return snapshot; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("status", status)
            .add("snapshot", snapshot)
            .toString();
    }
}
