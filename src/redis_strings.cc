//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_strings.h"

#include <memory>

#include "src/strings_filter.h"
#include "src/scope_record_lock.h"

namespace blackwidow {

Status RedisStrings::Open(const rocksdb::Options& options,
    const std::string& db_path) {
  rocksdb::Options ops(options);
  ops.compaction_filter_factory = std::make_shared<StringsFilterFactory>();
  return rocksdb::DB::Open(ops, db_path, &db_);
}

Status RedisStrings::Set(const Slice& key, const Slice& value) {
  InternalStringsValue internal_value(value);
  ScopeRecordLock l(lock_mgr_, key);
  return db_->Put(default_write_options_, key, internal_value.Encode());
}

Status RedisStrings::Get(const Slice& key, std::string* value) {
  Status s = db_->Get(default_read_options_, key, value);
  if (s.ok()) {
    ParsedInternalStringsValue internal_value(value);
    if (internal_value.IsStale()) {
      value->clear();
      return Status::NotFound("Stale");
    } else {
      internal_value.StripSuffix();
    }
  }
  return s;
}

Status RedisStrings::Setnx(const Slice& key, const Slice& value, int32_t* ret) {
  *ret = 0;
  std::string old_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &old_value);
  if (s.ok()) {
    ParsedInternalStringsValue internal_value(&old_value);
    if (internal_value.IsStale()) {
      InternalStringsValue new_value(value);
      s = db_->Put(default_write_options_, key, new_value.Encode());
      if (s.ok()) {
        *ret = 1;
      }
    }
  } else if (s.IsNotFound()) {
    InternalStringsValue new_value(value);
    s = db_->Put(default_write_options_, key, new_value.Encode());
    if (s.ok()) {
      *ret = 1;
    }
  }
  return s;
}

Status RedisStrings::Append(const Slice& key, const Slice& value, int32_t* ret) {
  std::string old_value;
  *ret = 0;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &old_value);
  if (s.ok()) {
    ParsedInternalStringsValue internal_value(&old_value);
    if (internal_value.IsStale()) {
      *ret = value.size();
      InternalStringsValue new_value(value);
      return db_->Put(default_write_options_, key, new_value.Encode());
    } else {
      internal_value.StripSuffix();
      *ret = old_value.size() + value.size();
      old_value += value.data();
      InternalStringsValue new_value(old_value);
      return db_->Put(default_write_options_, key, new_value.Encode());
    }
  } else if (s.IsNotFound()) {
    *ret = value.size();
    InternalStringsValue internal_value(value);
    return db_->Put(default_write_options_, key, internal_value.Encode());
  }
  return s;
}

Status RedisStrings::Expire(const Slice& key, int32_t ttl) {
  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedInternalStringsValue parsed(&value);
    if (parsed.IsStale()) {
      return Status::NotFound("Stale");
    }

    if (ttl > 0) {
      parsed.SetRelativeTimestamp(ttl);
      return db_->Put(default_write_options_, key, value);
    } else {
      return db_->Delete(default_write_options_, key);
    }
  }
  return s;
}

Status RedisStrings::CompactRange(const rocksdb::Slice* begin,
    const rocksdb::Slice* end) {
  return db_->CompactRange(default_compact_range_options_, begin, end);
}

}  //  namespace blackwidow
