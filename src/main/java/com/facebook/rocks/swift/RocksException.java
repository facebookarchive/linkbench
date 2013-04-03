package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

@ThriftStruct("RocksException")
public class RocksException extends Exception
{
    private static final long serialVersionUID = 1L;

private byte[] msg;

@ThriftField(value=1, name="msg")
public byte[] getMsg() { return msg; }

@ThriftField(value=1, name="msg")
public void setMsg(final byte[] msg) { this.msg = msg; }

private Code errorCode;

@ThriftField(value=2, name="errorCode")
public Code getErrorCode() { return errorCode; }

@ThriftField(value=2, name="errorCode")
public void setErrorCode(final Code errorCode) { this.errorCode = errorCode; }
}
