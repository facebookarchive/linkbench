package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

@ThriftStruct("WriteOptions")
public class WriteOptions
{
private boolean sync;

@ThriftField(value=1, name="sync")
public boolean isSync() { return sync; }

@ThriftField(value=1, name="sync")
public void setSync(final boolean sync) { this.sync = sync; }

private boolean disableWAL;

@ThriftField(value=2, name="disableWAL")
public boolean isDisableWAL() { return disableWAL; }

@ThriftField(value=2, name="disableWAL")
public void setDisableWAL(final boolean disableWAL) { this.disableWAL = disableWAL; }
}