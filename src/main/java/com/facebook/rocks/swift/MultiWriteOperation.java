package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("MultiWriteOperation")
public class MultiWriteOperation
{
    @ThriftConstructor
    public MultiWriteOperation(
        @ThriftField(value=1, name="opType") final OpType opType,
        @ThriftField(value=2, name="data") final Kv data
    ) {
        this.opType = opType;
        this.data = data;
    }

    private final OpType opType;

    @ThriftField(value=1, name="opType")
    public OpType getOpType() { return opType; }

    private final Kv data;

    @ThriftField(value=2, name="data")
    public Kv getData() { return data; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("opType", opType)
            .add("data", data)
            .toString();
    }
}
