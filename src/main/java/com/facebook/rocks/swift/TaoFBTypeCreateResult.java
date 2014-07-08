package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import java.util.*;

import static com.google.common.base.Objects.toStringHelper;

@ThriftStruct("TaoFBTypeCreateResult")
public class TaoFBTypeCreateResult
{
    @ThriftConstructor
    public TaoFBTypeCreateResult(
        @ThriftField(value=1, name="retCode") final RetCode retCode,
        @ThriftField(value=2, name="fbid") final long fbid
    ) {
        this.retCode = retCode;
        this.fbid = fbid;
    }

    private final RetCode retCode;

    @ThriftField(value=1, name="retCode")
    public RetCode getRetCode() { return retCode; }

    private final long fbid;

    @ThriftField(value=2, name="fbid")
    public long getFbid() { return fbid; }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("retCode", retCode)
            .add("fbid", fbid)
            .toString();
    }
}
