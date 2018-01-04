//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "blackwidow/blackwidow.h"

#include "src/redis_string.h"

namespace blackwidow {

BlackWidow::BlackWidow() :
    string_db_(nullptr) {
}

BlackWidow::~BlackWidow() {
  delete string_db_;
}

Status BlackWidow::Compact() {
  return string_db_->CompactRange(NULL, NULL);
}

static std::string AppendSubDirectory(const std::string& db_path,
    const std::string& sub_db) {
  if (db_path.back() == '/') {
    return db_path + sub_db;
  } else {
    return db_path + "/" + sub_db;
  }
}

Status BlackWidow::Open(const rocksdb::Options& options,
    const std::string& db_path) {
  string_db_ = new RedisString(rocksdb::Env::Default());
  Status s = string_db_->Open(options, AppendSubDirectory(db_path, "string"));
  return s;
}

Status BlackWidow::Set(const std::string& key, const std::string& value) {
  return string_db_->Set(key, value);
}

Status BlackWidow::Get(const std::string& key, std::string* value) {
  return string_db_->Get(key, value);
}

Status BlackWidow::Expire(const std::string& key, int32_t ttl) {
  return string_db_->Expire(key, ttl);
}

}  //  namespace blackwidow
