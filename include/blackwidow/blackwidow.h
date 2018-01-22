//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_BLACKWIDOW_BLACKWIDOW_H_
#define INCLUDE_BLACKWIDOW_BLACKWIDOW_H_

#include <string>

#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace blackwidow {
using Options = rocksdb::Options;
using Status = rocksdb::Status;
using Slice = rocksdb::Slice;

class RedisStrings;
class RedisHashes;
class BlackWidow {
 public:
  BlackWidow();
  ~BlackWidow();

  // Just For Test, will be removed later;
  Status Compact();

  Status Open(const Options& options, const std::string& db_path);

  // Strings Commands
  Status Set(const Slice& key, const Slice& value);
  Status Get(const Slice& key, std::string* value);
  Status Setnx(const Slice& key, const Slice& value, int32_t* ret);
  Status Append(const Slice& key, const Slice& value, int32_t* ret);
  Status Setex(const Slice& key, const Slice& value, int32_t ttl);
  Status Strlen(const Slice& key, int32_t* len);

  // Hashes Commands
  Status HSet(const Slice& key, const Slice& field, const Slice& value,
              int32_t* res);
  Status HGet(const Slice& key, const Slice& field, std::string* value);

  // Keys Commands
  Status Expire(const Slice& key, int32_t ttl);
  Status Delete(const Slice& key);

 private:
  RedisStrings* strings_db_;
  RedisHashes* hashes_db_;
};

}  //  namespace blackwidow
#endif  //  INCLUDE_BLACKWIDOW_BLACKWIDOW_H_
