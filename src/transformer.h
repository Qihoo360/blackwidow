//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_TRANSFORMER_H_
#define SRC_TRANSFORMER_H_

#include <string>

#include "src/coding.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"

namespace blackwidow {
using Slice = rocksdb::Slice;
class Transformer {
 public:
  explicit Transformer(rocksdb::Env* env) : env_(env) {
  }

  ~Transformer() = default;

  void AppendTimestamp(const std::string& value, int32_t ttl,
        std::string* value_with_ts) {
    int64_t unix_time;
    char ts_string[kTSLength];
    if (ttl <= 0) {
      EncodeFixed32(ts_string, 0);
    } else {
      env_->GetCurrentTime(&unix_time);
      EncodeFixed32(ts_string, (static_cast<int32_t>(unix_time) + ttl));
    }
    value_with_ts->append(value.data(), value.size());
    value_with_ts->append(ts_string, kTSLength);
  }

  bool IsStaleAndStripTimestamp(std::string* value_with_ts) {
    if (value_with_ts->size() < kTSLength) {
      return false;
    }
    int32_t timestamp = DecodeFixed32(value_with_ts->data() +
                  value_with_ts->size() - kTSLength);
    int64_t curtime;
    env_->GetCurrentTime(&curtime);
    if (timestamp == 0 || timestamp > curtime) {
      value_with_ts->erase(value_with_ts->size() - kTSLength, kTSLength);
      return false;
    }
    value_with_ts->clear();
    return true;
  }

  bool IsStale(const Slice& value_with_ts) {
    if (value_with_ts.size() < kTSLength) {
      return false;
    }
    int32_t timestamp = DecodeFixed32(value_with_ts.data() +
                  value_with_ts.size() - kTSLength);
    int64_t curtime;
    env_->GetCurrentTime(&curtime);
    return timestamp < curtime;
  }

  rocksdb::Env* env_;

  static const uint32_t kTSLength = sizeof(int32_t);
  // 05/09/2013:5:40PM GMT-8
  // static const int32_t kMinTimestamp = 1368146402;
  // 01/18/2038:7:14PM GMT-8
  // static const int32_t kMaxTimestamp = 2147483647;
};

}  //  namespace blackwidow
#endif  // SRC_TRANSFORMER_H_
