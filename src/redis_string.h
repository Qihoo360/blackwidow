//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_STRING_H_
#define SRC_REDIS_STRING_H_

#include <string>

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "src/string_converter.h"

namespace blackwidow {
using Status = rocksdb::Status;

class RedisString {
 public:
  explicit RedisString(rocksdb::Env* env)
    : converter_(env),
      db_(nullptr) {
    default_compact_range_options_.exclusive_manual_compaction = false;
    default_compact_range_options_.change_level = true;
  }

  ~RedisString() {
    delete db_;
  }

  Status Open(const rocksdb::Options& options, const std::string& db_path);
  Status Set(const std::string& key, const std::string& value);
  Status Get(const std::string& key, std::string* value);
  Status CompactRange(const rocksdb::Slice* begin,
      const rocksdb::Slice* end);

 private:
  StringConverter converter_;
  rocksdb::DB* db_;
  rocksdb::WriteOptions default_write_options_;
  rocksdb::ReadOptions default_read_options_;
  rocksdb::CompactRangeOptions default_compact_range_options_;
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_STRING_H_
