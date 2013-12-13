package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("RetCode")
public class RetCode
{
    @ThriftConstructor
    public RetCode(
        @ThriftField(value=1, name="state") final Code state,
        @ThriftField(value=2, name="msg") final byte [] msg
    ) {
        this.state = state;
        this.msg = msg;
    }

    private final Code state;

    @ThriftField(value=1, name="state")
    public Code getState() { return state; }

    private final byte [] msg;

    @ThriftField(value=2, name="msg")
    public byte [] getMsg() { return msg; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("state", state)
            .add("msg", msg)
            .toString();
    }
}
