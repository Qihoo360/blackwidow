//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_SETS_DATA_KEY_FORMAT_H_
#define SRC_SETS_DATA_KEY_FORMAT_H_

#include <string>
std::hash<std::string> hash_func;

namespace blackwidow {
class SetsMemberKey {
  public:
    SetsMemberKey(const Slice& key, int32_t version, const Slice& member) :
      start_(nullptr), key_(key), version_(version),
      serial_num_(static_cast<int32_t>(hash_func(member.ToString()))), member_(member) {
    }

    ~SetsMemberKey() {
      if (start_ != space_) {
        delete start_;
      }
    }

    const Slice Encode() {
      size_t usize = key_.size() + member_.size();
      size_t needed = usize + sizeof(int32_t) * 3;
      char* dst;
      if (needed < sizeof(space_)) {
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
      EncodeFixed32(dst, serial_num_);
      dst += sizeof(int32_t);
      memcpy(dst, member_.data(), member_.size());
      return Slice(start_, needed);
    }

    static void EncodePrefix(const Slice& key, int32_t version, std::string* prefix) {
      char* dst;
      char* start;
      size_t needed = key.size() + sizeof(int32_t) * 2;

      dst = new char[needed];
      start = dst;
      EncodeFixed32(dst, key.size());
      dst += sizeof(int32_t);
      memcpy(dst, key.data(), key.size());
      dst += key.size();
      EncodeFixed32(dst, version);
      dst += sizeof(int32_t);
      prefix->assign(start, needed);
      delete start;
    }

  private:
    char space_[200];
    char* start_;
    Slice key_;
    int32_t version_;
    int32_t serial_num_;
    Slice member_;
};

class ParsedSetsMemberKey {
  public:
    explicit ParsedSetsMemberKey(const std::string* rdb_key) {
      const char* ptr = rdb_key->data();
      int32_t key_len = DecodeFixed32(ptr);
      ptr += sizeof(int32_t);
      key_ = Slice(ptr, key_len);
      ptr += key_len;
      version_ = DecodeFixed32(ptr);
      ptr += sizeof(int32_t);
      serial_num_ = DecodeFixed32(ptr);
      ptr += sizeof(int32_t);
      member_ = Slice(ptr, rdb_key->size() - key_len - sizeof(int32_t) * 3);
    }

    explicit ParsedSetsMemberKey(const Slice& rdb_key) {
      const char* ptr = rdb_key.data();
      int32_t key_len = DecodeFixed32(ptr);
      ptr += sizeof(int32_t);
      key_ = Slice(ptr, key_len);
      ptr += key_len;
      version_ = DecodeFixed32(ptr);
      ptr += sizeof(int32_t);
      serial_num_ = DecodeFixed32(ptr);
      ptr += sizeof(int32_t);
      member_ = Slice(ptr, rdb_key.size() - key_len - sizeof(int32_t) * 3);
    }

    virtual ~ParsedSetsMemberKey() = default;

    Slice key() {
      return key_;
    }

    int32_t version() {
      return version_;
    }

    Slice member() {
      return member_;
    }

  private:
    Slice key_;
    int32_t version_;
    int32_t serial_num_;
    Slice member_;
};

}  //  namespace blackwidow
#endif  // SRC_SETS_DATA_KEY_FORMAT_H_
