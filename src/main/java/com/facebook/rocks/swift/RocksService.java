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


    @ThriftMethod(value = "TaoAssocPut",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    long TaoAssocPut(
        @ThriftField(value=1, name="tableName") final byte [] tableName,
        @ThriftField(value=2, name="assocType") final long assocType,
        @ThriftField(value=3, name="id1") final long id1,
        @ThriftField(value=4, name="id2") final long id2,
        @ThriftField(value=5, name="timestamp") final long timestamp,
        @ThriftField(value=6, name="visibility") final AssocVisibility visibility,
        @ThriftField(value=7, name="update_count") final boolean updateCount,
        @ThriftField(value=8, name="dataVersion") final long dataVersion,
        @ThriftField(value=9, name="data") final byte [] data,
        @ThriftField(value=10, name="wormhole_comment") final byte [] wormholeComment,
        @ThriftField(value=11, name="options") final WriteOptions options
    ) throws IOError;

    @ThriftMethod(value = "TaoAssocDelete",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    long TaoAssocDelete(
        @ThriftField(value=1, name="tableName") final byte [] tableName,
        @ThriftField(value=2, name="assocType") final long assocType,
        @ThriftField(value=3, name="id1") final long id1,
        @ThriftField(value=4, name="id2") final long id2,
        @ThriftField(value=5, name="visibility") final AssocVisibility visibility,
        @ThriftField(value=6, name="update_count") final boolean updateCount,
        @ThriftField(value=7, name="wormhole_comment") final byte [] wormholeComment,
        @ThriftField(value=8, name="options") final WriteOptions options
    ) throws IOError;

    @ThriftMethod(value = "TaoAssocRangeGet",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    List<TaoAssocGetResult> TaoAssocRangeGet(
        @ThriftField(value=1, name="tableName") final byte [] tableName,
        @ThriftField(value=2, name="assocType") final long assocType,
        @ThriftField(value=3, name="id1") final long id1,
        @ThriftField(value=4, name="start_time") final long startTime,
        @ThriftField(value=5, name="end_time") final long endTime,
        @ThriftField(value=6, name="offset") final long offset,
        @ThriftField(value=7, name="limit") final long limit
    ) throws IOError;

    @ThriftMethod(value = "TaoAssocGet",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    List<TaoAssocGetResult> TaoAssocGet(
        @ThriftField(value=1, name="tableName") final byte [] tableName,
        @ThriftField(value=2, name="assocType") final long assocType,
        @ThriftField(value=3, name="id1") final long id1,
        @ThriftField(value=4, name="id2s") final List<Long> id2s
    ) throws IOError;

    @ThriftMethod(value = "TaoAssocCount",
                  exception = {
                      @ThriftException(type=IOError.class, id=1)
                  })
    long TaoAssocCount(
        @ThriftField(value=1, name="tableName") final byte [] tableName,
        @ThriftField(value=2, name="assocType") final long assocType,
        @ThriftField(value=3, name="id1") final long id1
    ) throws IOError;
}