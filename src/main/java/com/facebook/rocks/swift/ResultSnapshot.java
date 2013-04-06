package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

@ThriftStruct("ResultSnapshot")
public class ResultSnapshot
{
private RetCode status;

@ThriftField(value=1, name="status")
public RetCode getStatus() { return status; }

@ThriftField(value=1, name="status")
public void setStatus(final RetCode status) { this.status = status; }

private Snapshot snapshot;

@ThriftField(value=2, name="snapshot")
public Snapshot getSnapshot() { return snapshot; }

@ThriftField(value=2, name="snapshot")
public void setSnapshot(final Snapshot snapshot) { this.snapshot = snapshot; }
}
