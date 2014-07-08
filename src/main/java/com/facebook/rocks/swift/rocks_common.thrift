// Copyright 2012 Facebook

namespace cpp facebook.rocks
namespace java facebook.rocks
namespace java.swift com.facebook.rocks.swift
namespace php rocks
namespace py rocks_common

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
  kEnsembleError = 11,
  kWriterAlreadyExistsError = 12,
  kReplicationError = 13,
  kRetryableError = 14,
  kTimeoutError = 15,
  kOpTooLarge = 16,
  kQueryFailed = 17,
}

struct RetCode {
  1: Code state,
  2: Slice msg,
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

/**
 * Response from fbtype_get
 */
struct TaoFBTypeGetResult {
  1: RetCode retCode,

  2:i32 fbtype,

  // Deletion flags
  3:i32 flags,

  // Creation time
  4:i64 ctime,
}

/**
 * Response from fbtype_create
 */
struct TaoFBTypeCreateResult {
  1: RetCode retCode,

  // Creation time
  2:i64 fbid,
}

struct TaoFBObjectGetResult {
  1: RetCode retCode,

  2:i32 version,

  3:i64 updateTime,

  4:Text data,
}

/**
 * Holds the assoc get result of a id2
 */
struct TaoAssocGetEntry {
  /** id2 of assoc */
  2:i64 id2,

  /** time stamp of the assoc */
  3:i64 time,

  /** version of the data blob */
  4:i64 version,

  /** serialized data of the assoc */
  5:Text data,
}

struct TaoAssocGetResult {
  1: RetCode retCode,

  2: list<TaoAssocGetEntry> entries,
}

struct TaoAssocCountResult {
  1: RetCode retCode,

  2: i64 count,
}

service RocksServiceCommon {

}
