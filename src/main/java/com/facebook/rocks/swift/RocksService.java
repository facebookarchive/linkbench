package com.facebook.rocks.swift;

import com.facebook.swift.codec.*;
import com.facebook.swift.service.*;
import java.io.*;
import java.util.*;

@ThriftService("RocksService")
public interface RocksService extends Closeable
{
    void close();


    @ThriftMethod(value = "Put")
    RetCode Put(
        @ThriftField(value=1, name="dbname") final byte [] dbname,
        @ThriftField(value=2, name="key") final byte [] key,
        @ThriftField(value=3, name="value") final byte [] value,
        @ThriftField(value=4, name="options") final WriteOptions options
    );


    @ThriftMethod(value = "Delete")
    RetCode Delete(
        @ThriftField(value=1, name="dbname") final byte [] dbname,
        @ThriftField(value=2, name="key") final byte [] key,
        @ThriftField(value=3, name="options") final WriteOptions options
    );


    @ThriftMethod(value = "MultiWrite")
    RetCode MultiWrite(
        @ThriftField(value=1, name="dbname") final byte [] dbname,
        @ThriftField(value=2, name="batch") final List<MultiWriteOperation> batch,
        @ThriftField(value=3, name="options") final WriteOptions options
    );


    @ThriftMethod(value = "Get")
    RocksGetResponse Get(
        @ThriftField(value=1, name="dbname") final byte [] dbname,
        @ThriftField(value=2, name="inputkey") final byte [] inputkey,
        @ThriftField(value=3, name="options") final ReadOptions options
    );


    @ThriftMethod(value = "MultiGet")
    RocksMultiGetResponse MultiGet(
        @ThriftField(value=1, name="dbname") final byte [] dbname,
        @ThriftField(value=2, name="inputkeys") final List<byte []> inputkeys,
        @ThriftField(value=3, name="options") final ReadOptions options
    );


    @ThriftMethod(value = "Iterate")
    RocksIterateResponse Iterate(
        @ThriftField(value=1, name="dbname") final byte [] dbname,
        @ThriftField(value=2, name="startKey") final byte [] startKey,
        @ThriftField(value=3, name="endKey") final byte [] endKey,
        @ThriftField(value=4, name="options") final ReadOptions options,
        @ThriftField(value=5, name="max") final int max,
        @ThriftField(value=6, name="includeEndKey") final boolean includeEndKey
    );


    @ThriftMethod(value = "CreateSnapshot")
    ResultSnapshot CreateSnapshot(
        @ThriftField(value=1, name="dbname") final byte [] dbname,
        @ThriftField(value=2, name="startKey") final byte [] startKey
    );


    @ThriftMethod(value = "ReleaseSnapshot")
    RetCode ReleaseSnapshot(
        @ThriftField(value=1, name="dbname") final byte [] dbname,
        @ThriftField(value=2, name="snapshot") final Snapshot snapshot
    );


    @ThriftMethod(value = "CompactRange")
    RetCode CompactRange(
        @ThriftField(value=1, name="dbname") final byte [] dbname,
        @ThriftField(value=2, name="start") final byte [] start,
        @ThriftField(value=3, name="endhere") final byte [] endhere
    );


    @ThriftMethod(value = "GetApproximateSize")
    long GetApproximateSize(
        @ThriftField(value=1, name="dbname") final byte [] dbname,
        @ThriftField(value=2, name="start") final byte [] start,
        @ThriftField(value=3, name="endhere") final byte [] endhere
    );


    @ThriftMethod(value = "isEmpty")
    boolean isEmpty();


    @ThriftMethod(value = "Noop")
    void Noop();


    @ThriftMethod(value = "TaoFBTypeGet",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    TaoFBTypeGetResult TaoFBTypeGet(
        @ThriftField(value=1, name="fbid") final long fbid
    ) throws IOError;

    @ThriftMethod(value = "TaoFBTypeCreate",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    TaoFBTypeCreateResult TaoFBTypeCreate(
        @ThriftField(value=1, name="dbid") final int dbid,
        @ThriftField(value=2, name="ctime") final long ctime,
        @ThriftField(value=3, name="fbtype") final int fbtype,
        @ThriftField(value=4, name="wormhole_comment") final byte [] wormholeComment,
        @ThriftField(value=5, name="woptions") final WriteOptions woptions
    ) throws IOError;

    @ThriftMethod(value = "TaoFBTypeResurrect",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    RetCode TaoFBTypeResurrect(
        @ThriftField(value=1, name="fbid") final long fbid,
        @ThriftField(value=2, name="fbtype") final int fbtype,
        @ThriftField(value=3, name="deletion_flags") final int deletionFlags,
        @ThriftField(value=4, name="wormhole_comment") final byte [] wormholeComment,
        @ThriftField(value=5, name="woptions") final WriteOptions woptions
    ) throws IOError;

    @ThriftMethod(value = "TaoFBTypeDel",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    RetCode TaoFBTypeDel(
        @ThriftField(value=1, name="fbid") final long fbid,
        @ThriftField(value=2, name="fbtype") final int fbtype,
        @ThriftField(value=3, name="flags") final int flags,
        @ThriftField(value=4, name="wormhole_comment") final byte [] wormholeComment,
        @ThriftField(value=5, name="woptions") final WriteOptions woptions
    ) throws IOError;

    @ThriftMethod(value = "TaoFBObjectPut",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    RetCode TaoFBObjectPut(
        @ThriftField(value=1, name="fbid") final long fbid,
        @ThriftField(value=2, name="fbtype") final int fbtype,
        @ThriftField(value=3, name="version") final int version,
        @ThriftField(value=4, name="new_version") final int newVersion,
        @ThriftField(value=5, name="time") final long time,
        @ThriftField(value=6, name="data") final byte [] data,
        @ThriftField(value=7, name="is_create") final boolean isCreate,
        @ThriftField(value=8, name="wormhole_comment") final byte [] wormholeComment,
        @ThriftField(value=9, name="woptions") final WriteOptions woptions
    ) throws IOError;

    @ThriftMethod(value = "TaoFBObjectGet",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    TaoFBObjectGetResult TaoFBObjectGet(
        @ThriftField(value=1, name="fbid") final long fbid,
        @ThriftField(value=2, name="fbtype") final int fbtype
    ) throws IOError;

    @ThriftMethod(value = "TaoFBObjectDel",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    RetCode TaoFBObjectDel(
        @ThriftField(value=1, name="fbid") final long fbid,
        @ThriftField(value=2, name="fbtype") final int fbtype,
        @ThriftField(value=3, name="wormhole_comment") final byte [] wormholeComment,
        @ThriftField(value=4, name="woptions") final WriteOptions woptions
    ) throws IOError;

    @ThriftMethod(value = "TaoAssocPut",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    TaoAssocCountResult TaoAssocPut(
        @ThriftField(value=1, name="tableName") final byte [] tableName,
        @ThriftField(value=2, name="assocType") final long assocType,
        @ThriftField(value=3, name="id1") final long id1,
        @ThriftField(value=4, name="id2") final long id2,
        @ThriftField(value=5, name="timestamp") final long timestamp,
        @ThriftField(value=6, name="visibility") final AssocVisibility visibility,
        @ThriftField(value=7, name="update_count") final boolean updateCount,
        @ThriftField(value=8, name="version") final long version,
        @ThriftField(value=9, name="data") final byte [] data,
        @ThriftField(value=10, name="wormhole_comment") final byte [] wormholeComment,
        @ThriftField(value=11, name="woptions") final WriteOptions woptions
    ) throws IOError;

    @ThriftMethod(value = "TaoAssocDelete",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    TaoAssocCountResult TaoAssocDelete(
        @ThriftField(value=1, name="tableName") final byte [] tableName,
        @ThriftField(value=2, name="assocType") final long assocType,
        @ThriftField(value=3, name="id1") final long id1,
        @ThriftField(value=4, name="id2") final long id2,
        @ThriftField(value=5, name="version") final long version,
        @ThriftField(value=6, name="visibility") final AssocVisibility visibility,
        @ThriftField(value=7, name="update_count") final boolean updateCount,
        @ThriftField(value=8, name="wormhole_comment") final byte [] wormholeComment,
        @ThriftField(value=9, name="woptions") final WriteOptions woptions
    ) throws IOError;

    @ThriftMethod(value = "TaoAssocGetTimeRange",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    TaoAssocGetResult TaoAssocGetTimeRange(
        @ThriftField(value=1, name="tableName") final byte [] tableName,
        @ThriftField(value=2, name="assocType") final long assocType,
        @ThriftField(value=3, name="id1") final long id1,
        @ThriftField(value=4, name="startTime") final long startTime,
        @ThriftField(value=5, name="endTime") final long endTime,
        @ThriftField(value=6, name="offset") final long offset,
        @ThriftField(value=7, name="limit") final long limit
    ) throws IOError;

    @ThriftMethod(value = "TaoAssocGetCursorRange",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    TaoAssocGetResult TaoAssocGetCursorRange(
        @ThriftField(value=1, name="tableName") final byte [] tableName,
        @ThriftField(value=2, name="assocType") final long assocType,
        @ThriftField(value=3, name="id1") final long id1,
        @ThriftField(value=4, name="id2") final long id2,
        @ThriftField(value=5, name="time") final long time,
        @ThriftField(value=6, name="offset") final long offset,
        @ThriftField(value=7, name="limit") final long limit
    ) throws IOError;

    @ThriftMethod(value = "TaoAssocGetID2s",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    TaoAssocGetResult TaoAssocGetID2s(
        @ThriftField(value=1, name="tableName") final byte [] tableName,
        @ThriftField(value=2, name="assocType") final long assocType,
        @ThriftField(value=3, name="id1") final long id1,
        @ThriftField(value=4, name="id2s") final List<Long> id2s
    ) throws IOError;

    @ThriftMethod(value = "TaoAssocCount",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    TaoAssocCountResult TaoAssocCount(
        @ThriftField(value=1, name="tableName") final byte [] tableName,
        @ThriftField(value=2, name="assocType") final long assocType,
        @ThriftField(value=3, name="id1") final long id1
    ) throws IOError;

    @ThriftMethod(value = "TaoAssocCountPut",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    TaoAssocCountResult TaoAssocCountPut(
        @ThriftField(value=1, name="tableName") final byte [] tableName,
        @ThriftField(value=2, name="assocType") final long assocType,
        @ThriftField(value=3, name="id1") final long id1,
        @ThriftField(value=4, name="count") final long count,
        @ThriftField(value=5, name="wormhole_comment") final byte [] wormholeComment,
        @ThriftField(value=6, name="woptions") final WriteOptions woptions
    ) throws IOError;

    @ThriftMethod(value = "InvalidateKeys",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    RetCode InvalidateKeys(
        @ThriftField(value=1, name="keys") final byte [] keys,
        @ThriftField(value=2, name="woptions") final WriteOptions woptions
    ) throws IOError;
}