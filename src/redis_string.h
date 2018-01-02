//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_STRING_H_
#define SRC_REDIS_STRING_H_

#include <string>

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/slice.h"
#include "rocksdb/compaction_filter.h"
#include "src/transformer.h"

namespace blackwidow {
using Status = rocksdb::Status;
using Slice = rocksdb::Slice;

class RedisString {
 public:
  RedisString(const std::string& db_path, const rocksdb::Options& options,
        Transformer* transformer,
        rocksdb::CompactionFilterFactory* factory)
    : db_path_(db_path + "string"),
      options_(options),
      transformer_(transformer),
      factory_(factory),
      db_(nullptr) {
    default_compact_range_options_.exclusive_manual_compaction = false;
    default_compact_range_options_.change_level = true;
  }

  ~RedisString() {
    delete db_;
  }

  const rocksdb::DB* db() {
    return db_;
  }

  Status Open();
  Status Set(const std::string& key, const std::string& value);
  Status Get(const std::string& key, std::string* value);
  Status CompactRange(const rocksdb::Slice* begin,
      const rocksdb::Slice* end);

 private:
  std::string db_path_;
  rocksdb::Options options_;
  Transformer* transformer_;
  rocksdb::CompactionFilterFactory* factory_;
  rocksdb::DB* db_;
  rocksdb::WriteOptions default_write_options_;
  rocksdb::ReadOptions default_read_options_;
  rocksdb::CompactRangeOptions default_compact_range_options_;
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_STRING_H_
