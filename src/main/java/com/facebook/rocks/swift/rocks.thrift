// Copyright 2012 Facebook

include "rocks_common.thrift"

namespace cpp facebook.rocks
namespace java facebook.rocks
namespace java.swift com.facebook.rocks.swift
namespace php rocks
namespace py rocks

typedef binary Text
typedef binary Bytes

typedef binary Slice

const string kVersionHeader = "version";
const string kShardKeyRange = "keyrange";

exception RocksException {
  1:Text msg,
  2:i32 errorCode
}

//
// An IOError exception from an assoc operation
//
exception IOError {
  1:string message
}

// Different compression types supported
enum CompressionType {
  kNoCompression     = 0x0,
  kSnappyCompression = 0x1,
  kZlib = 0x2,
  kBZip2 = 0x3
}

enum OpType {
  kPut    = 0x0,
  kDelete = 0x1
}

struct RocksMultiGetResponse {
  1: rocks_common.RetCode retCode,
  2: list<rocks_common.RocksGetResponse> gets
}

struct MultiWriteOperation {
  1: OpType opType,
  2: rocks_common.kv data
}

// Options for writing
struct WriteOptions {
  1:bool sync,
  2:bool disableWAL,
}

struct Snapshot {
  1:i64 snapshotid     // server generated
}

// Snapshot result
struct ResultSnapshot {
  1:rocks_common.RetCode status,
  2:Snapshot snapshot
}

// Options for reading. If you do not have a
// snapshot, set snapshot.snapshotid = 0
struct ReadOptions {
  1:bool verify_checksums,
  2:bool fill_cache,
  3:Snapshot snapshot
}

//
// Visibility state for assoc
//
enum AssocVisibility
{
  VISIBLE = 0, // live object, include in lookups and count
  DELETED = 1, // exclude from lookup queries and count, ok to
               // delete permanently from persistent store
  UNUSED1 = 2,  // not used
  HIDDEN = 3,  // exclude from lookup queries and count
  UNUSED2 = 4, // not used
  HARD_DELETE = 5 // deleted by calling expunge, will be swept
                  // as soon as possible
}

service RocksService {
  // puts a key in the database
  rocks_common.RetCode Put(1:Text dbname,
              2:Slice key,
              3:Slice value,
              4:WriteOptions options),

  // deletes a key from the database
  rocks_common.RetCode Delete(1:Text dbname,
                 2:Slice key,
                 3:WriteOptions options),

  // Processes the specified batch of puts & deletes.
  rocks_common.RetCode MultiWrite(1:Text dbname,
                     2:list<MultiWriteOperation> batch,
                     3:WriteOptions options),

  // fetch a key from the DB.
  // RocksResponse.status == kNotFound means key does non exist
  // RocksResponse.status == kOk means key is found
  rocks_common.RocksGetResponse Get(1:Text dbname,
                       2:Slice inputkey,
                       3:ReadOptions options),

  // Batched get of the specified keys.
  // RocksMultiGetResponse.retCode.status is set to kOk if no error was
  // encountered while processing the batch else it is set to the error that was
  // encountered and no values are returned.  In the event in which everything
  // was successful, the responses are returned as a collection of
  // RocksGetResponse (one per requested key).  RocksGetResponse.retCode.status
  // is set to kOk if the key was found else it is set to kNotFound.
  RocksMultiGetResponse MultiGet(1:Text dbname,
                                 2:list<Slice> inputkeys,
                                 3:ReadOptions options),

  // fetch a range of KVs in the range specified by startKey and endKey.
  // startKey is always included while endKey is included only if includeEndKey
  // is set.
  // startKey gives the start key.
  // endKey gives the end key.
  // RocksIterateResponse.status == kOK means more data.
  // RocksIterateResponse.status == kEnd means no data.
  // All other return status means errors.
  rocks_common.RocksIterateResponse Iterate(1:Text dbname,
                               2:Slice startKey,
                               3:Slice endKey,
                               4:ReadOptions options,
                               5:i32 max,
                               6:bool includeEndKey),

  // Create snapshot.
  ResultSnapshot CreateSnapshot(1:Text dbname,
                                2:Slice startKey),

  // Release snapshots
  rocks_common.RetCode ReleaseSnapshot(1:Text dbname,
                          2:Snapshot snapshot),

  // compact a range of keys
  // begin.size == 0 to start at a range earlier than the first existing key
  // end.size == 0 to end at a range later than the last existing key
  rocks_common.RetCode CompactRange(1:Text dbname,
                       2:Slice start,
                       3:Slice endhere),

  i64 GetApproximateSize(
    1: Text dbname,
    2: Slice start,
    3: Slice endhere
  ),

  bool isEmpty(),

  void Noop(),

  // fbtype related
  rocks_common.TaoFBTypeGetResult TaoFBTypeGet(
    // fbid info to get
    1:i64 fbid,
  ) throws (1:IOError io);

  rocks_common.TaoFBTypeCreateResult TaoFBTypeCreate(
    // dbid
    1:i32 dbid,

    // Creation time
    2:i64 ctime,

    // Type of fbid to create
    3:i32 fbtype,

    /** wormhole comment */
    4:Text wormhole_comment,

    5:WriteOptions woptions,
  ) throws (1:IOError io);

  rocks_common.RetCode TaoFBTypeResurrect(
    // FBID to resurrect
    1:i64 fbid,

    // FbType
    2:i32 fbtype,

    // Deletion flags
    3:i32 deletion_flags,

    /** wormhole comment */
    4:Text wormhole_comment,

    5:WriteOptions woptions,
  ) throws (1:IOError io);

  rocks_common.RetCode TaoFBTypeDel(
    // fbid to delete
    1:i64 fbid,

    // FbType
    2:i32 fbtype,

    // Type of delete to perform
    3:i32 flags,

    /** wormhole comment */
    4:Text wormhole_comment,

    5:WriteOptions woptions,
  ) throws (1:IOError io);

  rocks_common.RetCode TaoFBObjectPut(
    // fbid to delete
    1:i64 fbid,

    // FbType of the fbid
    2:i32 fbtype,

    // version
    3:i32 version,

    // new version
    4:i32 new_version,

    // time
    5:i64 time,

    // data
    6:Text data,

    // is create
    7:bool is_create,

    /** wormhole comment */
    8:Text wormhole_comment,

    9:WriteOptions woptions,
  ) throws (1:IOError io);

  rocks_common.TaoFBObjectGetResult TaoFBObjectGet(
    // fbid to delete
    1:i64 fbid,

    // FbType of the fbid
    2:i32 fbtype,
  ) throws (1:IOError io);

  rocks_common.RetCode TaoFBObjectDel(
    // fbid to delete
    1:i64 fbid,

    // FbType of the fbid
    2:i32 fbtype,

    /** wormhole comment */
    3:Text wormhole_comment,

    4:WriteOptions woptions,
  ) throws (1:IOError io);

  /**
   * TAO Assoc Put operation.
   * Note that currently the argument visibility has no effect.
   *
   * @if update_count is true, then return the updated count for this assoc
   * @if update_count is false, then return 0
   * @return negative number if failure
   */
  rocks_common.TaoAssocCountResult TaoAssocPut(
    /** name of table */
    1:Text tableName,

    /** type assoc */
    2:i64 assocType,

    /** id1 of assoc */
    3:i64 id1,

    /** id2 of assoc */
    4:i64 id2,

    /** timestamp of assoc */
    5:i64 timestamp,

    /** visibility */
    6:AssocVisibility visibility,

    /** whether to keep the count or not */
    7:bool update_count,

    /** version of the data blob */
    8:i64 version,

    /** serialized data of assoc */
    9:Text data,

    /** wormhole comment */
    10:Text wormhole_comment,

    11:WriteOptions woptions,
  ) throws (1:IOError io),

 /**
  * TAO Assoc Delete operation.
  *
  * @return the updated count for this assoc
  */
  rocks_common.TaoAssocCountResult TaoAssocDelete(
    /** name of table */
    1:Text tableName,

    /** type assoc */
    2:i64 assocType,

    /** id1 of assoc */
    3:i64 id1,

    /** id2 of assoc */
    4:i64 id2,

    5:i64 version,

    /** visibility flag for this delete */
    6:AssocVisibility visibility,

    /** whether to keep the count or not */
    7:bool update_count,

    /** wormhole comment */
    8:Text wormhole_comment,

    9:WriteOptions woptions,
  ) throws (1:IOError io),

  /**
   * TAO Assoc Get TimeRange operation.
   * Obtain assocs in bewteen starTime and endTime in the given order.
   * The range check is inclusive: startTime <= time && time <= endTime.
   */
  rocks_common.TaoAssocGetResult TaoAssocGetTimeRange(
    /** name of table */
    1:Text tableName,

    /** type of assoc */
    2:i64 assocType,

    /** id1 of assoc */
    3:i64 id1,

    /** maximum timestamp of assocs to retrieve */
    4:i64 startTime,

    /** minimum timestamp of assocs to retrieve */
    5:i64 endTime,

    /** number of assocs to skip from start */
    6:i64 offset,

    /** max number of assocs (columns) returned */
    7:i64 limit
  ) throws (1:IOError io),

  /**
   * TAO Assoc Get CursorRange operation.
   * Obtain assocs after <time, id2> in the given order.
   */
  rocks_common.TaoAssocGetResult TaoAssocGetCursorRange(
    /** name of table */
    1:Text tableName,

    /** type of assoc */
    2:i64 assocType,

    /** id1 of assoc */
    3:i64 id1,

    /** cursor: id2 **/
    4:i64 id2,

    /** cursor: time */
    5:i64 time,

    /** number of assocs to skip from start */
    6:i64 offset,

    /** max number of assocs (columns) returned */
    7:i64 limit
  ) throws (1:IOError io),

  /**
   * TAO Assoc Get operation.
   * Obtain assocs with the given id2s
   */
  rocks_common.TaoAssocGetResult TaoAssocGetID2s(
    /** name of table */
    1:Text tableName,

    /** type of assoc */
    2:i64 assocType,

    /** id1 of assoc */
    3:i64 id1,

    /** list of id2 need to be fetch */
    4:list<i64> id2s
  ) throws (1:IOError io),

  /**
   * TAO Assoc Count Get operation.
   * Returns the number of assocs for given id1 and assoc type
   */
  rocks_common.TaoAssocCountResult TaoAssocCount(
    /** name of table */
    1:Text tableName,

    /** type of assoc */
    2:i64 assocType,

    /** id1 of assoc */
    3:i64 id1,
  ) throws (1:IOError io),

  rocks_common.TaoAssocCountResult TaoAssocCountPut(
    /** name of table */
    1:Text tableName,

    /** type of assoc */
    2:i64 assocType,

    /** id1 of assoc */
    3:i64 id1,

    4:i64 count,

    /** wormhole comment */
    5:Text wormhole_comment,

    6:WriteOptions woptions,
  ) throws (1:IOError io),

  rocks_common.RetCode InvalidateKeys(
    /** keys to invalidate */
    1:Text keys,

    2:WriteOptions woptions,
  ) throws (1:IOError io),
}
