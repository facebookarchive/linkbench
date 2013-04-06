// Copyright 2012 Facebook
//The thirft file that was used to auto-generate the code for RocksDb

namespace cpp facebook.rocks
namespace java facebook.rocks
namespace php rocks
namespace py rocks

// Types
typedef binary Text
typedef binary Bytes

typedef binary Slice

enum Code {
  kOk = 0,
  kEnd = 1,
  kNotFound = 2,
  kCorruption = 3,
  kNotSupported = 4,
  kInvalidArgument = 5,
  kIOError = 6,
  kSnapshotNotExists = 7,
  kWrongShard = 8,
  kUnknownError = 9,
  kClientError = 10,
}

struct RetCode {
  1: Code state,
  2: Slice msg,
}

exception RocksException {
  1:Text msg,
  2:Code errorCode
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

/**
 * Holds the assoc get result of a id2
 */
struct TaoAssocGetResult {
  /** id2 of assoc */
  1:i64 id2,

  /** time stamp of the assoc */
  4:i64 time,

  /** version of the data blob */
  5:i64 dataVersion,

  /** serialized data of the asoc */
  6:Text data,
}


struct RocksGetResponse {
  1: RetCode retCode,
  2: Slice value
}

struct kv {
  1:Slice key,
  2:Slice value
}

struct RocksIterateResponse {
  1: RetCode status,
  2: list<kv> data,
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
  1:RetCode status,
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
  RetCode Put(1:Text dbname,
              2:Slice key,
              3:Slice value,
              4:WriteOptions options),

  // deletes a key from the database
  RetCode Delete(1:Text dbname,
                 2:Slice key,
                 3:WriteOptions options),

  // writes batch of keys into the database
  RetCode Write(1:Text dbname,
                2:list<kv> batch,
                3:WriteOptions options),

  // fetch a key from the DB.
  // RocksResponse.status == kNotFound means key does non exist
  // RocksResponse.status == kOk means key is found
  RocksGetResponse Get(1:Text dbname,
                       2:Slice inputkey,
                       3:ReadOptions options),

  // fetch a range of KVs.
  // startKey gives the start key.
  // endKey gives the end key.
  // RocksIterateResponse.status == kOK means more data.
  // RocksIterateResponse.status == kEnd means no data.
  // All other return status means errors.
  RocksIterateResponse Iterate(1:Text dbname,
                               2:Slice startKey,
                               3:Slice endKey,
                               4:ReadOptions options,
                               5:i32 max),

  // Create snapshot.
  ResultSnapshot CreateSnapshot(1:Text dbname,
                                2:Slice startKey),

  // Release snapshots
  RetCode ReleaseSnapshot(1:Text dbname,
                          2:Snapshot snapshot),

  // compact a range of keys
  // begin.size == 0 to start at a range earlier than the first existing key
  // end.size == 0 to end at a range later than the last existing key
  RetCode CompactRange(1:Text dbname,
                       2:Slice start,
                       3:Slice endhere),

  bool Empty(),

  void Noop(),

  /**
   * TAO Assoc Put operation.
   * Note that currently the argument visibility has no effect.
   *
   * @if update_count is true, then return the updated count for this assoc
   * @if update_count is false, then return 0
   * @return negative number if failure
   */
  i64 TaoAssocPut(
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
    8:i64 dataVersion,

    /** serialized data of assoc */
    9:Text data,

    /** wormhole comment */
    10:Text wormhole_comment
  ) throws (1:IOError io),

 /**
  * TAO Assoc Delete operation.
  *
  * @return the updated count for this assoc
  */
  i64 TaoAssocDelete(
    /** name of table */
    1:Text tableName,

    /** type assoc */
    2:i64 assocType,

    /** id1 of assoc */
    3:i64 id1,

    /** id2 of assoc */
    4:i64 id2,

    /** visibility flag for this delete */
    5:AssocVisibility visibility,

    /** whether to keep the count or not */
    6:bool update_count,

    /** wormhole comment */
    7:Text wormhole_comment
  ) throws (1:IOError io),

  /**
   * TAO Assoc RangeGet operation.
   * Obtain assocs in bewteen start_time and end_time in reverse time order.
   * The range check is inclusive: start_time >= time && time >= end_time.
   * And yes, start_time >= end_time because this range scan is a backward
   * scan in time, starting with most recent time and scanning backwards
   * for the most recent n assocs.
   */
  list<TaoAssocGetResult> TaoAssocRangeGet(
    /** name of table */
    1:Text tableName,

    /** type of assoc */
    2:i64 assocType,

    /** id1 of assoc */
    3:i64 id1,

    /** maximum timestamp of assocs to retrieve */
    4:i64 start_time,

    /** minimum timestamp of assocs to retrieve */
    5:i64 end_time,

    /** number of assocs to skip from start */
    6:i64 offset,

    /** max number of assocs (columns) returned */
    7:i64 limit
  ) throws (1:IOError io),

  /**
   * TAO Assoc Get operation.
   */
  list<TaoAssocGetResult> TaoAssocGet(
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
  i64 TaoAssocCount(
    /** name of table */
    1:Text tableName,

    /** type of assoc */
    2:i64 assocType,

    /** id1 of assoc */
    3:i64 id1,
  ) throws (1:IOError io),
}
