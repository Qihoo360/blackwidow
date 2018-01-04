//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_STRING_H_
#define SRC_REDIS_STRING_H_

#include <string>
#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "src/string_converter.h"
#include "src/lock_mgr.h"
#include "src/mutex_impl.h"

namespace blackwidow {
using Status = rocksdb::Status;

class RedisString {
 public:
  explicit RedisString(rocksdb::Env* env)
    : converter_(env),
      lock_mgr_(new LockMgr(1000, 10000, std::make_shared<MutexFactoryImpl>())),
      db_(nullptr) {
    default_compact_range_options_.exclusive_manual_compaction = false;
    default_compact_range_options_.change_level = true;
  }

  ~RedisString() {
    delete db_;
    delete lock_mgr_;
  }

  Status Open(const rocksdb::Options& options, const std::string& db_path);
  Status Set(const std::string& key, const std::string& value);
  Status Get(const std::string& key, std::string* value);
  Status Expire(const std::string& key, int32_t ttl);
  Status CompactRange(const rocksdb::Slice* begin,
      const rocksdb::Slice* end);

 private:
  StringConverter converter_;
  LockMgr* lock_mgr_;
  rocksdb::DB* db_;
  rocksdb::WriteOptions default_write_options_;
  rocksdb::ReadOptions default_read_options_;
  rocksdb::CompactRangeOptions default_compact_range_options_;
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_STRING_H_
