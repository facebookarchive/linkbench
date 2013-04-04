package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

@ThriftStruct("RetCode")
public class RetCode
{
private Code state;

@ThriftField(value=1, name="state")
public Code getState() { return state; }

@ThriftField(value=1, name="state")
public void setState(final Code state) { this.state = state; }

private byte[] msg;

@ThriftField(value=2, name="msg")
public byte[] getMsg() { return msg; }

@ThriftField(value=2, name="msg")
public void setMsg(final byte[] msg) { this.msg = msg; }
}
