//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_HASHES_DATA_KEY_FORMAT_H_
#define SRC_HASHES_DATA_KEY_FORMAT_H_

#include <string>

namespace blackwidow {
class HashesDataKey {
 public:
  HashesDataKey(const Slice& key, int32_t version, const Slice& field) :
    start_(nullptr), key_(key), version_(version), field_(field) {
  }

  ~HashesDataKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  const Slice Encode() {
    size_t usize = key_.size() + field_.size();
    size_t needed = usize + sizeof(int32_t) * 2;
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];
    }
    start_ = dst;
    EncodeFixed32(dst, key_.size());
    dst += sizeof(int32_t);
    memcpy(dst, key_.data(), key_.size());
    dst += key_.size();
    EncodeFixed32(dst, version_);
    dst += sizeof(int32_t);
    memcpy(dst, field_.data(), field_.size());
    return Slice(start_, needed);
  }

 private:
  char space_[200];
  char* start_;
  Slice key_;
  int32_t version_;
  Slice field_;
};

class ParsedHashesDataKey {
 public:
  explicit ParsedHashesDataKey(const std::string* key) {
    const char* ptr = key->data();
    int32_t key_len = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    field_ = Slice(ptr, key->size() - key_len - sizeof(int32_t) * 2);
  }

  explicit ParsedHashesDataKey(const Slice& key) {
    const char* ptr = key.data();
    int32_t key_len = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    field_ = Slice(ptr, key.size() - key_len - sizeof(int32_t) * 2);
  }

  virtual ~ParsedHashesDataKey() = default;

  Slice key() {
    return key_;
  }

  int32_t version() {
    return version_;
  }

  Slice field() {
    return field_;
  }

 private:
  std::string* raw_key_;
  Slice key_;
  int32_t version_;
  Slice field_;
};

}  //  namespace blackwidow
#endif  // SRC_HASHES_DATA_KEY_FORMAT_H_
