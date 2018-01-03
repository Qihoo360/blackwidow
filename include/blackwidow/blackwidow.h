//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_BLACKWIDOW_H_
#define INCLUDE_BLACKWIDOW_H_

#include <string>

#include "rocksdb/status.h"
#include "rocksdb/options.h"

namespace blackwidow {
using Options = rocksdb::Options;
using Status = rocksdb::Status;

class RedisString;
class BlackWidow {
 public:
  explicit BlackWidow();
  ~BlackWidow();

  Status Open(const Options& options, const std::string& db_path);
  Status Set(const std::string& key, const std::string& value);
  Status Get(const std::string& key, std::string* value);

 private:
  RedisString* string_db_;
};

}  //  namespace blackwidow
#endif  //  INCLUDE_BLACKWIDOW_H_
