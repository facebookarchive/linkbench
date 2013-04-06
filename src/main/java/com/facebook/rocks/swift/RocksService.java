package com.facebook.rocks.swift;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.service.ThriftException;
import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

import java.io.Closeable;

import java.util.List;

@ThriftService("RocksService")
public interface RocksService extends Closeable {
  @ThriftMethod(value = "Put")
  RetCode Put(@ThriftField(value = 1, name = "dbname") byte[] dbname,
              @ThriftField(value = 2, name = "key") byte[] key,
              @ThriftField(value = 3, name = "value") byte[] value,
              @ThriftField(value = 4, name = "options") WriteOptions options);

  @ThriftMethod(value = "Delete")
  RetCode Delete(@ThriftField(value = 1, name = "dbname") byte[] dbname,
                 @ThriftField(value = 2, name = "key") byte[] key,
                 @ThriftField(value = 3, name = "options") WriteOptions options);

  @ThriftMethod(value = "Write")
  RetCode Write(@ThriftField(value = 1, name = "dbname") byte[] dbname,
                @ThriftField(value = 2, name = "batch") List<Kv> batch,
                @ThriftField(value = 3, name = "options") WriteOptions options);

  @ThriftMethod(value = "Get")
  RocksGetResponse Get(@ThriftField(value = 1, name = "dbname") byte[] dbname,
                       @ThriftField(value = 2, name = "inputkey") byte[] inputkey,
                       @ThriftField(value = 3, name = "options") ReadOptions options);

  @ThriftMethod(value = "Iterate")
  RocksIterateResponse Iterate(@ThriftField(value = 1, name = "dbname") byte[] dbname,
                               @ThriftField(value = 2, name = "startKey") byte[] startKey,
                               @ThriftField(value = 3, name = "endKey") byte[] endKey,
                               @ThriftField(value = 4, name = "options") ReadOptions options,
                               @ThriftField(value = 5, name = "max") int max);

  @ThriftMethod(value = "CreateSnapshot")
  ResultSnapshot CreateSnapshot(@ThriftField(value = 1, name = "dbname") byte[] dbname,
                                @ThriftField(value = 2, name = "startKey") byte[] startKey);

  @ThriftMethod(value = "ReleaseSnapshot")
  RetCode ReleaseSnapshot(@ThriftField(value = 1, name = "dbname") byte[] dbname,
                          @ThriftField(value = 2, name = "snapshot") Snapshot snapshot);

  @ThriftMethod(value = "CompactRange")
  RetCode CompactRange(@ThriftField(value = 1, name = "dbname") byte[] dbname,
                       @ThriftField(value = 2, name = "start") byte[] start,
                       @ThriftField(value = 3, name = "endhere") byte[] endhere);

  @ThriftMethod(value = "Empty")
  boolean Empty();

  @ThriftMethod(value = "Noop")
  void Noop();

  @ThriftMethod(value = "TaoAssocPut",
                exception = {@ThriftException(type = IOError.class, id = 1)})
  long TaoAssocPut(@ThriftField(value = 1, name = "tableName") byte[] tableName,
                   @ThriftField(value = 2, name = "assocType") long assocType,
                   @ThriftField(value = 3, name = "id1") long id1,
                   @ThriftField(value = 4, name = "id2") long id2,
                   @ThriftField(value = 5, name = "timestamp") long timestamp,
                   @ThriftField(value = 6, name = "visibility") AssocVisibility visibility,
                   @ThriftField(value = 7, name = "update_count") boolean updateCount,
                   @ThriftField(value = 8, name = "dataVersion") long dataVersion,
                   @ThriftField(value = 9, name = "data") byte[] data,
                   @ThriftField(value = 10, name = "wormhole_comment") byte[] wormholeComment)
    throws IOError;

  @ThriftMethod(value = "TaoAssocDelete",
                exception = {@ThriftException(type = IOError.class, id = 1)})
  long TaoAssocDelete(@ThriftField(value = 1, name = "tableName") byte[] tableName,
                      @ThriftField(value = 2, name = "assocType") long assocType,
                      @ThriftField(value = 3, name = "id1") long id1,
                      @ThriftField(value = 4, name = "id2") long id2,
                      @ThriftField(value = 5, name = "visibility") AssocVisibility visibility,
                      @ThriftField(value = 6, name = "update_count") boolean updateCount,
                      @ThriftField(value = 7, name = "wormhole_comment") byte[] wormholeComment)
    throws IOError;

  @ThriftMethod(value = "TaoAssocRangeGet",
                exception = {@ThriftException(type = IOError.class, id = 1)})
  List<TaoAssocGetResult> TaoAssocRangeGet(@ThriftField(value = 1,
                                                        name = "tableName") byte[] tableName,
                                           @ThriftField(value = 2, name = "assocType") long assocType,
                                           @ThriftField(value = 3, name = "id1") long id1,
                                           @ThriftField(value = 4, name = "start_time") long startTime,
                                           @ThriftField(value = 5, name = "end_time") long endTime,
                                           @ThriftField(value = 6, name = "offset") long offset,
                                           @ThriftField(value = 7, name = "limit") long limit)
    throws IOError;

  @ThriftMethod(value = "TaoAssocGet",
                exception = {@ThriftException(type = IOError.class, id = 1)})
  List<TaoAssocGetResult> TaoAssocGet(@ThriftField(value = 1, name = "tableName") byte[] tableName,
                                      @ThriftField(value = 2, name = "assocType") long assocType,
                                      @ThriftField(value = 3, name = "id1") long id1,
                                      @ThriftField(value = 4, name = "id2s") List<Long> id2s)
    throws IOError;

  @ThriftMethod(value = "TaoAssocCount",
                exception = {@ThriftException(type = IOError.class, id = 1)})
  long TaoAssocCount(@ThriftField(value = 1, name = "tableName") byte[] tableName,
                     @ThriftField(value = 2, name = "assocType") long assocType,
                     @ThriftField(value = 3, name = "id1") long id1)
    throws IOError;
}
