package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

@ThriftStruct("TaoAssocGetResult")
public class TaoAssocGetResult
{
private long id1, id2;

@ThriftField(value=1, name="id1")
public long getId1() { return id1; }

@ThriftField(value=1, name="id1")
public void setId1(final long id1) { this.id1 = id1; }

@ThriftField(value=1, name="id2")
public long getId2() { return id2; }

@ThriftField(value=1, name="id2")
public void setId2(final long id2) { this.id2 = id2; }

private long time;

@ThriftField(value=4, name="time")
public long getTime() { return time; }

@ThriftField(value=4, name="time")
public void setTime(final long time) { this.time = time; }

private long dataVersion;

@ThriftField(value=5, name="dataVersion")
public long getDataVersion() { return dataVersion; }

@ThriftField(value=5, name="dataVersion")
public void setDataVersion(final long dataVersion) { this.dataVersion = dataVersion; }

private byte[] data;

@ThriftField(value=6, name="data")
public byte[] getData() { return data; }

@ThriftField(value=6, name="data")
public void setData(final byte[] data) { this.data = data; }
}
