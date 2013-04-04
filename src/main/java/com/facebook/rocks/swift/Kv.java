package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

@ThriftStruct("Kv")
public class Kv
{
private byte[] key;

@ThriftField(value=1, name="key")
public byte[] getKey() { return key; }

@ThriftField(value=1, name="key")
public void setKey(final byte[] key) { this.key = key; }

private byte[] value;

@ThriftField(value=2, name="value")
public byte[] getValue() { return value; }

@ThriftField(value=2, name="value")
public void setValue(final byte[] value) { this.value = value; }
}
