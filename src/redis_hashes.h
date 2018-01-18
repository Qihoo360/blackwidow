//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_HASHES_H_
#define SRC_REDIS_HASHES_H_

#include <string>
#include <vector>

#include "src/redis.h"

namespace blackwidow {

class RedisHashes : public Redis {
 public:
  RedisHashes() = default;
  ~RedisHashes();

  // Hashes Commands
  Status HSet(const Slice& key, const Slice& field, const Slice& value,
      int32_t* res);
  Status HGet(const Slice& key, const Slice& field, std::string* value);

  // Common Commands
  virtual Status Open(const rocksdb::Options& options,
      const std::string& db_path) override;
  virtual Status CompactRange(const rocksdb::Slice* begin,
      const rocksdb::Slice* end) override;

  // Keys Commands
  virtual Status Expire(const Slice& key, int32_t ttl) override;
 private:
  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_HASHES_H_
