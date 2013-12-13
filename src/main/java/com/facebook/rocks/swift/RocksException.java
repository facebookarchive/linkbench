package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

@ThriftStruct("RocksException")
public class RocksException extends Exception
{
    private static final long serialVersionUID = 1L;

    @ThriftConstructor
    public RocksException(
        @ThriftField(value=1, name="msg") final byte [] msg,
        @ThriftField(value=2, name="errorCode") final int errorCode
    ) {
        this.msg = msg;
        this.errorCode = errorCode;
    }

    private final byte [] msg;

    @ThriftField(value=1, name="msg")
    public byte [] getMsg() { return msg; }

    private final int errorCode;

    @ThriftField(value=2, name="errorCode")
    public int getErrorCode() { return errorCode; }
}
