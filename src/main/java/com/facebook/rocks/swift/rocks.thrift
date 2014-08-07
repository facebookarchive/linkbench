// Copyright 2012 Facebook

include "rocks_common.thrift"
//include "common/fb303/if/fb303.thrift"

namespace cpp facebook.rocks
namespace cpp2 facebook.rocks
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
  1:string msg,
}

// Options for writing
struct WriteOptions {
  1:bool sync,
  2:bool disableWAL,
}

struct Snapshot {
  1:i64 snapshotid     // server generated
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

service RocksService {//extends fb303.FacebookService {
  // fbtype related
  rocks_common.TaoFBTypeGetResult TaoFBTypeGet(
    // fbid info to get
    1:i64 fbid,
  ) throws (1:RocksException io);

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
  ) throws (1:RocksException io);

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
  ) throws (1:RocksException io);

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
  ) throws (1:RocksException io);

  void TaoFBObjectPut(
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
  ) throws (1:RocksException io);

  rocks_common.TaoFBObjectGetResult TaoFBObjectGet(
    // fbid to delete
    1:i64 fbid,

    // FbType of the fbid
    2:i32 fbtype,
  ) throws (1:RocksException io);

  void TaoFBObjectDel(
    // fbid to delete
    1:i64 fbid,

    // FbType of the fbid
    2:i32 fbtype,

    /** wormhole comment */
    3:Text wormhole_comment,

    4:WriteOptions woptions,
  ) throws (1:RocksException io);

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
    8:i64 version,

    /** serialized data of assoc */
    9:Text data,

    /** wormhole comment */
    10:Text wormhole_comment,

    11:WriteOptions woptions,
  ) throws (1:RocksException io),

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

    5:i64 version,

    /** visibility flag for this delete */
    6:AssocVisibility visibility,

    /** whether to keep the count or not */
    7:bool update_count,

    /** wormhole comment */
    8:Text wormhole_comment,

    9:WriteOptions woptions,
  ) throws (1:RocksException io),

  /**
   * TAO Assoc Get TimeRange operation.
   * Obtain assocs in bewteen starTime and endTime in the given order.
   * The range check is inclusive: startTime <= time && time <= endTime.
   */
  list<rocks_common.TaoAssocGetEntry> TaoAssocGetTimeRange(
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
  ) throws (1:RocksException io),

  /**
   * TAO Assoc Get CursorRange operation.
   * Obtain assocs after <time, id2> in the given order.
   */
  list<rocks_common.TaoAssocGetEntry> TaoAssocGetCursorRange(
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
  ) throws (1:RocksException io),

  /**
   * TAO Assoc Get operation.
   * Obtain assocs with the given id2s
   */
  list<rocks_common.TaoAssocGetEntry> TaoAssocGetID2s(
    /** name of table */
    1:Text tableName,

    /** type of assoc */
    2:i64 assocType,

    /** id1 of assoc */
    3:i64 id1,

    /** list of id2 need to be fetch */
    4:list<i64> id2s
  ) throws (1:RocksException ex),

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
  ) throws (1:RocksException ex),

  i64 TaoAssocCountPut(
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
  ) throws (1:RocksException io),
}
