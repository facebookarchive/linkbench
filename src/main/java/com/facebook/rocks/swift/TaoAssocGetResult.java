package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("TaoAssocGetResult")
public class TaoAssocGetResult
{
    @ThriftConstructor
    public TaoAssocGetResult(
        @ThriftField(value=1, name="id2") final long id2,
        @ThriftField(value=4, name="time") final long time,
        @ThriftField(value=5, name="dataVersion") final long dataVersion,
        @ThriftField(value=6, name="data") final byte [] data
    ) {
        this.id2 = id2;
        this.time = time;
        this.dataVersion = dataVersion;
        this.data = data;
    }

    private final long id2;

    @ThriftField(value=1, name="id2")
    public long getId2() { return id2; }

    private final long time;

    @ThriftField(value=4, name="time")
    public long getTime() { return time; }

    private final long dataVersion;

    @ThriftField(value=5, name="dataVersion")
    public long getDataVersion() { return dataVersion; }

    private final byte [] data;

    @ThriftField(value=6, name="data")
    public byte [] getData() { return data; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("id2", id2)
            .add("time", time)
            .add("dataVersion", dataVersion)
            .add("data", data)
            .toString();
    }
}
