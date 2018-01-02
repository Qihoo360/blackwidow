//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_string.h"

namespace blackwidow {

Status RedisString::Open() {
  return rocksdb::DB::Open(options_, db_path_, &db_);
}

Status RedisString::Set(const std::string& key, const std::string& value) {
  std::string value_with_ts;
  transformer_->AppendTimestamp(value, 1, &value_with_ts);
  return db_->Put(default_write_options_, key, value_with_ts);
}

Status RedisString::Get(const std::string& key, std::string* value) {
  Status s = db_->Get(default_read_options_, key, value);
  if (s.ok() && transformer_->IsStaleAndStripTimestamp(value)) {
    return Status::NotFound("Stale");
  }
  return s;
}

Status RedisString::CompactRange(const rocksdb::Slice* begin,
    const rocksdb::Slice* end) {
  return db_->CompactRange(default_compact_range_options_, begin, end);
}

}  //  namespace blackwidow
