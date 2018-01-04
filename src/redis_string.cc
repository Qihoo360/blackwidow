//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_string.h"

#include <memory>

#include "src/string_filter.h"
#include "src/scope_record_lock.h"

namespace blackwidow {

Status RedisString::Open(const rocksdb::Options& options,
    const std::string& db_path) {
  rocksdb::Options ops(options);
  ops.compaction_filter_factory = std::make_shared<StringFilterFactory>(
      &converter_);
  return rocksdb::DB::Open(ops, db_path, &db_);
}

Status RedisString::Set(const std::string& key, const std::string& value) {
  std::string value_with_ts;
  converter_.AppendTimestamp(value, 0, &value_with_ts);
  ScopeRecordLock l(lock_mgr_, key);
  return db_->Put(default_write_options_, key, value_with_ts);
}

Status RedisString::Get(const std::string& key, std::string* value) {
  Status s = db_->Get(default_read_options_, key, value);
  if (s.ok()) {
    if (converter_.IsStale(*value)) {
      value->clear();
      return Status::NotFound("Stale");
    } else {
      converter_.StripTimestamp(value);
    }
  }
  return s;
}

Status RedisString::Expire(const std::string& key, int32_t ttl) {
  std::string value_with_ts;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value_with_ts);
  if (s.ok()) {
    if (converter_.IsStale(value_with_ts)) {
      value_with_ts.clear();
      return Status::NotFound("Stale");
    } else {
      converter_.StripTimestamp(&value_with_ts);
    }

    if (ttl > 0) {
      std::string value_with_new_ts;
      converter_.AppendTimestamp(value_with_ts, ttl, &value_with_new_ts);
      return db_->Put(default_write_options_, key, value_with_new_ts);
    } else {
      return db_->Delete(default_write_options_, key);
    }
  }
  return s;
}

Status RedisString::CompactRange(const rocksdb::Slice* begin,
    const rocksdb::Slice* end) {
  return db_->CompactRange(default_compact_range_options_, begin, end);
}

}  //  namespace blackwidow
