package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

@ThriftStruct("RocksIterateResponse")
public class RocksIterateResponse
{
private RetCode status;

@ThriftField(value=1, name="status")
public RetCode getStatus() { return status; }

@ThriftField(value=1, name="status")
public void setStatus(final RetCode status) { this.status = status; }

private List<Kv> data;

@ThriftField(value=2, name="data")
public List<Kv> getData() { return data; }

@ThriftField(value=2, name="data")
public void setData(final List<Kv> data) { this.data = data; }
}