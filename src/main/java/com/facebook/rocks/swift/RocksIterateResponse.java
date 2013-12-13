package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("RocksIterateResponse")
public class RocksIterateResponse
{
    @ThriftConstructor
    public RocksIterateResponse(
        @ThriftField(value=1, name="status") final RetCode status,
        @ThriftField(value=2, name="data") final List<Kv> data
    ) {
        this.status = status;
        this.data = data;
    }

    private final RetCode status;

    @ThriftField(value=1, name="status")
    public RetCode getStatus() { return status; }

    private final List<Kv> data;

    @ThriftField(value=2, name="data")
    public List<Kv> getData() { return data; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("status", status)
            .add("data", data)
            .toString();
    }
}
