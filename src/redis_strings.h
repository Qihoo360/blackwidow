//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_STRINGS_H_
#define SRC_REDIS_STRINGS_H_

#include <string>
#include <memory>

#include "src/string_converter.h"
#include "src/redis.h"

namespace blackwidow {

class RedisStrings : public Redis {
 public:
  explicit RedisStrings(rocksdb::Env* env)
    : converter_(env) {
  }

  ~RedisStrings() = default;

  Status Set(const std::string& key, const std::string& value);
  Status Get(const std::string& key, std::string* value);

  virtual Status Open(const rocksdb::Options& options,
      const std::string& db_path) override;
  virtual Status Expire(const std::string& key, int32_t ttl) override;
  virtual Status CompactRange(const rocksdb::Slice* begin,
      const rocksdb::Slice* end) override;

 private:
  StringConverter converter_;
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_STRINGS_H_
