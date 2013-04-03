package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

@ThriftStruct("RocksGetResponse")
public class RocksGetResponse
{
private RetCode retCode;

@ThriftField(value=1, name="retCode")
public RetCode getRetCode() { return retCode; }

@ThriftField(value=1, name="retCode")
public void setRetCode(final RetCode retCode) { this.retCode = retCode; }

private byte[] value;

@ThriftField(value=2, name="value")
public byte[] getValue() { return value; }

@ThriftField(value=2, name="value")
public void setValue(final byte[] value) { this.value = value; }
}
