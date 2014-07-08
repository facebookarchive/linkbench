package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("TaoAssocGetEntry")
public class TaoAssocGetEntry
{
    @ThriftConstructor
    public TaoAssocGetEntry(
        @ThriftField(value=2, name="id2") final long id2,
        @ThriftField(value=3, name="time") final long time,
        @ThriftField(value=4, name="version") final long version,
        @ThriftField(value=5, name="data") final byte [] data
    ) {
        this.id2 = id2;
        this.time = time;
        this.version = version;
        this.data = data;
    }

    private final long id2;

    @ThriftField(value=2, name="id2")
    public long getId2() { return id2; }

    private final long time;

    @ThriftField(value=3, name="time")
    public long getTime() { return time; }

    private final long version;

    @ThriftField(value=4, name="version")
    public long getVersion() { return version; }

    private final byte [] data;

    @ThriftField(value=5, name="data")
    public byte [] getData() { return data; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("id2", id2)
            .add("time", time)
            .add("version", version)
            .add("data", data)
            .toString();
    }
}
