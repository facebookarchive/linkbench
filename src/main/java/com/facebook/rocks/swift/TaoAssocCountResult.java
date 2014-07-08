package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("TaoAssocCountResult")
public class TaoAssocCountResult
{
    @ThriftConstructor
    public TaoAssocCountResult(
        @ThriftField(value=1, name="retCode") final RetCode retCode,
        @ThriftField(value=2, name="count") final long count
    ) {
        this.retCode = retCode;
        this.count = count;
    }

    private final RetCode retCode;

    @ThriftField(value=1, name="retCode")
    public RetCode getRetCode() { return retCode; }

    private final long count;

    @ThriftField(value=2, name="count")
    public long getCount() { return count; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("retCode", retCode)
            .add("count", count)
            .toString();
    }
}
