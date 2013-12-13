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

service RocksServiceCommon {

}
