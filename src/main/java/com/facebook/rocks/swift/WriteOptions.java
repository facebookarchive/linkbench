package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("WriteOptions")
public class WriteOptions
{
    @ThriftConstructor
    public WriteOptions(
        @ThriftField(value=1, name="sync") final boolean sync,
        @ThriftField(value=2, name="disableWAL") final boolean disableWAL
    ) {
        this.sync = sync;
        this.disableWAL = disableWAL;
    }

    private final boolean sync;

    @ThriftField(value=1, name="sync")
    public boolean isSync() { return sync; }

    private final boolean disableWAL;

    @ThriftField(value=2, name="disableWAL")
    public boolean isDisableWAL() { return disableWAL; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("sync", sync)
            .add("disableWAL", disableWAL)
            .toString();
    }
}
