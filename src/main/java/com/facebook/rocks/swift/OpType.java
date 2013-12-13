package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;

public enum OpType
{
    K_PUT(0), K_DELETE(1);

    private final int value;

    OpType(int value)
    {
        this.value = value;
    }

    @ThriftEnumValue
    public int getValue()
    {
        return value;
    }
}
