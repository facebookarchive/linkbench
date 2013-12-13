package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("RocksMultiGetResponse")
public class RocksMultiGetResponse
{
    @ThriftConstructor
    public RocksMultiGetResponse(
        @ThriftField(value=1, name="retCode") final RetCode retCode,
        @ThriftField(value=2, name="gets") final List<RocksGetResponse> gets
    ) {
        this.retCode = retCode;
        this.gets = gets;
    }

    private final RetCode retCode;

    @ThriftField(value=1, name="retCode")
    public RetCode getRetCode() { return retCode; }

    private final List<RocksGetResponse> gets;

    @ThriftField(value=2, name="gets")
    public List<RocksGetResponse> getGets() { return gets; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("retCode", retCode)
            .add("gets", gets)
            .toString();
    }
}
