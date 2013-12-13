package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("Kv")
public class Kv
{
    @ThriftConstructor
    public Kv(
        @ThriftField(value=1, name="key") final byte [] key,
        @ThriftField(value=2, name="value") final byte [] value
    ) {
        this.key = key;
        this.value = value;
    }

    private final byte [] key;

    @ThriftField(value=1, name="key")
    public byte [] getKey() { return key; }

    private final byte [] value;

    @ThriftField(value=2, name="value")
    public byte [] getValue() { return value; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("key", key)
            .add("value", value)
            .toString();
    }
}
