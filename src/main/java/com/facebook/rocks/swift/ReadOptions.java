package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("ReadOptions")
public class ReadOptions
{
    @ThriftConstructor
    public ReadOptions(
        @ThriftField(value=1, name="verify_checksums") final boolean verifyChecksums,
        @ThriftField(value=2, name="fill_cache") final boolean fillCache,
        @ThriftField(value=3, name="snapshot") final Snapshot snapshot
    ) {
        this.verifyChecksums = verifyChecksums;
        this.fillCache = fillCache;
        this.snapshot = snapshot;
    }

    private final boolean verifyChecksums;

    @ThriftField(value=1, name="verify_checksums")
    public boolean isVerifyChecksums() { return verifyChecksums; }

    private final boolean fillCache;

    @ThriftField(value=2, name="fill_cache")
    public boolean isFillCache() { return fillCache; }

    private final Snapshot snapshot;

    @ThriftField(value=3, name="snapshot")
    public Snapshot getSnapshot() { return snapshot; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("verifyChecksums", verifyChecksums)
            .add("fillCache", fillCache)
            .add("snapshot", snapshot)
            .toString();
    }
}
