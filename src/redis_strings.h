//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_STRINGS_H_
#define SRC_REDIS_STRINGS_H_

#include <string>
#include <memory>

#include "src/redis.h"

namespace blackwidow {

class RedisStrings : public Redis {
 public:
  RedisStrings() = default;
  ~RedisStrings() = default;

  // Strings Commands
  Status Set(const Slice& key, const Slice& value);
  Status Get(const Slice& key, std::string* value);
  Status Setnx(const Slice& key, const Slice& value, int32_t* ret);
  Status Append(const Slice& key, const Slice& value, int32_t* ret);
  Status Setex(const Slice& key, const Slice& value, int32_t ttl);
  Status Strlen(const Slice& key, int32_t *len);

  // Common Commands
  virtual Status Open(const rocksdb::Options& options,
      const std::string& db_path) override;
  virtual Status CompactRange(const rocksdb::Slice* begin,
      const rocksdb::Slice* end) override;

  // Keys Commands
  virtual Status Expire(const Slice& key, int32_t ttl) override;
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_STRINGS_H_
