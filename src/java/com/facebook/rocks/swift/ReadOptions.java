package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

@ThriftStruct("ReadOptions")
public class ReadOptions
{
private boolean verifyChecksums;

@ThriftField(value=1, name="verify_checksums")
public boolean isVerifyChecksums() { return verifyChecksums; }

@ThriftField(value=1, name="verify_checksums")
public void setVerifyChecksums(final boolean verifyChecksums) { this.verifyChecksums = verifyChecksums; }

private boolean fillCache;

@ThriftField(value=2, name="fill_cache")
public boolean isFillCache() { return fillCache; }

@ThriftField(value=2, name="fill_cache")
public void setFillCache(final boolean fillCache) { this.fillCache = fillCache; }

private Snapshot snapshot;

@ThriftField(value=3, name="snapshot")
public Snapshot getSnapshot() { return snapshot; }

@ThriftField(value=3, name="snapshot")
public void setSnapshot(final Snapshot snapshot) { this.snapshot = snapshot; }
}