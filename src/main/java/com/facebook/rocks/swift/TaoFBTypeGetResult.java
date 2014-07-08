package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("TaoFBTypeGetResult")
public class TaoFBTypeGetResult
{
    @ThriftConstructor
    public TaoFBTypeGetResult(
        @ThriftField(value=1, name="retCode") final RetCode retCode,
        @ThriftField(value=2, name="fbtype") final int fbtype,
        @ThriftField(value=3, name="flags") final int flags,
        @ThriftField(value=4, name="ctime") final long ctime
    ) {
        this.retCode = retCode;
        this.fbtype = fbtype;
        this.flags = flags;
        this.ctime = ctime;
    }

    private final RetCode retCode;

    @ThriftField(value=1, name="retCode")
    public RetCode getRetCode() { return retCode; }

    private final int fbtype;

    @ThriftField(value=2, name="fbtype")
    public int getFbtype() { return fbtype; }

    private final int flags;

    @ThriftField(value=3, name="flags")
    public int getFlags() { return flags; }

    private final long ctime;

    @ThriftField(value=4, name="ctime")
    public long getCtime() { return ctime; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("retCode", retCode)
            .add("fbtype", fbtype)
            .add("flags", flags)
            .add("ctime", ctime)
            .toString();
    }
}
