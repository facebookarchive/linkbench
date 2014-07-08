package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("TaoFBObjectGetResult")
public class TaoFBObjectGetResult
{
    @ThriftConstructor
    public TaoFBObjectGetResult(
        @ThriftField(value=1, name="retCode") final RetCode retCode,
        @ThriftField(value=2, name="version") final int version,
        @ThriftField(value=3, name="updateTime") final long updateTime,
        @ThriftField(value=4, name="data") final byte [] data
    ) {
        this.retCode = retCode;
        this.version = version;
        this.updateTime = updateTime;
        this.data = data;
    }

    private final RetCode retCode;

    @ThriftField(value=1, name="retCode")
    public RetCode getRetCode() { return retCode; }

    private final int version;

    @ThriftField(value=2, name="version")
    public int getVersion() { return version; }

    private final long updateTime;

    @ThriftField(value=3, name="updateTime")
    public long getUpdateTime() { return updateTime; }

    private final byte [] data;

    @ThriftField(value=4, name="data")
    public byte [] getData() { return data; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("retCode", retCode)
            .add("version", version)
            .add("updateTime", updateTime)
            .add("data", data)
            .toString();
    }
}
