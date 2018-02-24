//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_H_
#define SRC_REDIS_H_

#include <string>
#include <memory>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/slice.h"
#include "src/lock_mgr.h"
#include "src/mutex_impl.h"

namespace blackwidow {
using Status = rocksdb::Status;
using Slice = rocksdb::Slice;

class Redis {
 public:
  Redis()
    : lock_mgr_(new LockMgr(1000, 10000, std::make_shared<MutexFactoryImpl>())),
      db_(nullptr) {
    default_compact_range_options_.exclusive_manual_compaction = false;
    default_compact_range_options_.change_level = true;
  }

  virtual ~Redis() {
    delete db_;
    delete lock_mgr_;
  }

  // Common Commands
  virtual Status Open(const rocksdb::Options& options,
      const std::string& db_path) = 0;
  virtual Status CompactRange(const rocksdb::Slice* begin,
      const rocksdb::Slice* end) = 0;

  // Keys Commands
  virtual Status Expire(const Slice& key, int32_t ttl) = 0;
  virtual Status Del(const Slice& key) = 0;
  virtual bool Scan(const std::string& start_key,
                    const std::string& pattern,
                    std::vector<std::string>* keys,
                    int64_t* count,
                    std::string* next_key) = 0;

 protected:
  LockMgr* lock_mgr_;
  rocksdb::DB* db_;
  rocksdb::WriteOptions default_write_options_;
  rocksdb::ReadOptions default_read_options_;
  rocksdb::CompactRangeOptions default_compact_range_options_;
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_H_
