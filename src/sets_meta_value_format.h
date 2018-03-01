//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_SETS_META_VALUE_FORMAT_H_
#define SRC_SETS_META_VALUE_FORMAT_H_

#include <string>

#include "src/base_value_format.h"

namespace blackwidow {

class SetsMetaValue : public InternalValue {
 public:
  explicit SetsMetaValue(const Slice& user_value) :
    InternalValue(user_value) {
  }
  virtual size_t AppendTimestampAndVersion() override {
    size_t usize = user_value_.size();
    char* dst = start_;
    memcpy(dst, user_value_.data(), usize);
    dst += usize;
    EncodeFixed32(dst, version_);
    dst += sizeof(int32_t);
    EncodeFixed32(dst, timestamp_);
    return usize + 2 * sizeof(int32_t);
  }

  int32_t UpdateVersion() {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (version_ >= static_cast<int32_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<int32_t>(unix_time);
    }
    return version_;
  }
};

class ParsedSetsMetaValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get();
  explicit ParsedSetsMetaValue(std::string* internal_value_str) :
    ParsedInternalValue(internal_value_str) {
    if (internal_value_str->size() >= kSetsMetaValueSuffixLength) {
      user_value_ = Slice(internal_value_str->data(),
          internal_value_str->size() - kSetsMetaValueSuffixLength);
      version_ = DecodeFixed32(internal_value_str->data() +
            internal_value_str->size() - sizeof(int32_t) * 2);
      timestamp_ = DecodeFixed32(internal_value_str->data() +
            internal_value_str->size() - sizeof(int32_t));
    }
    count_ = DecodeFixed32(internal_value_str->data());
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedSetsMetaValue(const Slice& internal_value_slice) :
    ParsedInternalValue(internal_value_slice) {
    if (internal_value_slice.size() >= kSetsMetaValueSuffixLength) {
      user_value_ = Slice(internal_value_slice.data(),
          internal_value_slice.size() - kSetsMetaValueSuffixLength);
      version_ = DecodeFixed32(internal_value_slice.data() +
            internal_value_slice.size() - sizeof(int32_t) * 2);
      timestamp_ = DecodeFixed32(internal_value_slice.data() +
            internal_value_slice.size() - sizeof(int32_t));
    }
    count_ = DecodeFixed32(internal_value_slice.data());
  }

  virtual void StripSuffix() override {
    if (value_ != nullptr) {
      value_->erase(value_->size() - kSetsMetaValueSuffixLength,
          kSetsMetaValueSuffixLength);
    }
  }

  virtual void SetVersionToValue() override {
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() -
        kSetsMetaValueSuffixLength;
      EncodeFixed32(dst, version_);
    }
  }

  virtual void SetTimestampToValue() override {
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() -
        sizeof(int32_t);
      EncodeFixed32(dst, timestamp_);
    }
  }
  static const size_t kSetsMetaValueSuffixLength = 2 * sizeof(int32_t);

  int32_t count() {
    return count_;
  }

  void set_count(int32_t count) {
    count_ = count;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed32(dst, count_);
    }
  }

  void ModifyCount(int32_t delta) {
    count_ += delta;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed32(dst, count_);
    }
  }

  int32_t UpdateVersion() {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (version_ >= static_cast<int32_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<int32_t>(unix_time);
    }
    SetVersionToValue();
    return version_;
  }

 private:
  int32_t count_;
};

}  //  namespace blackwidow
#endif  // SRC_SETS_META_VALUE_FORMAT_H_
