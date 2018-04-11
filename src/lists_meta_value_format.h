//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_LISTS_META_VALUE_FORMAT_H_
#define SRC_LISTS_META_VALUE_FORMAT_H_

#include <string>

#include "src/base_value_format.h"

namespace blackwidow {

class ListsMetaValue : public InternalValue {
 public:
  explicit ListsMetaValue(const Slice& user_value) :
    InternalValue(user_value),
    left_index_(2147483647),
    right_index_(2147483648) {
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

  virtual size_t AppendIndex() {
    char * dst = start_;
    dst += user_value_.size() + 2 * sizeof(int32_t);
    EncodeFixed32(dst, left_index_);
    dst += sizeof(int32_t);
    EncodeFixed32(dst, right_index_);
    return 2 * sizeof(int32_t);
  }

  static const size_t kDefaultValueSuffixLength = sizeof(int32_t) * 4;

  virtual const Slice Encode() override {
    size_t usize = user_value_.size();
    size_t needed = usize + kDefaultValueSuffixLength;
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];
    }
    start_  = dst;
    size_t len = AppendTimestampAndVersion() + AppendIndex();
    return Slice(start_, len);
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

  uint32_t left_index() {
    return left_index_;
  }

  void ModifyLeftIndex(uint32_t index) {
    left_index_ -= index;
  }

  uint32_t right_index() {
    return right_index_;
  }

  void ModifyRightIndex(uint32_t index) {
    right_index_ += index;
  }


 private:
  uint32_t left_index_;
  uint32_t right_index_;
};

class ParsedListsMetaValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get();
  explicit ParsedListsMetaValue(std::string* internal_value_str) :
    ParsedInternalValue(internal_value_str) {
    if (internal_value_str->size() >= kListsMetaValueSuffixLength) {
      user_value_ = Slice(internal_value_str->data(),
          internal_value_str->size() - kListsMetaValueSuffixLength);
      version_ = DecodeFixed32(internal_value_str->data() +
            internal_value_str->size() - sizeof(int32_t) * 4);
      timestamp_ = DecodeFixed32(internal_value_str->data() +
            internal_value_str->size() - sizeof(int32_t) * 3);
      left_index_ = DecodeFixed32(internal_value_str->data() +
            internal_value_str->size() - sizeof(int32_t) * 2);
      right_index_ = DecodeFixed32(internal_value_str->data() +
            internal_value_str->size() - sizeof(int32_t));
    }
    count_ = DecodeFixed32(internal_value_str->data());
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedListsMetaValue(const Slice& internal_value_slice) :
    ParsedInternalValue(internal_value_slice) {
    if (internal_value_slice.size() >= kListsMetaValueSuffixLength) {
      user_value_ = Slice(internal_value_slice.data(),
          internal_value_slice.size() - kListsMetaValueSuffixLength);
      version_ = DecodeFixed32(internal_value_slice.data() +
            internal_value_slice.size() - sizeof(int32_t) * 4);
      timestamp_ = DecodeFixed32(internal_value_slice.data() +
            internal_value_slice.size() - sizeof(int32_t) * 3);
      left_index_ = DecodeFixed32(internal_value_slice.data() +
            internal_value_slice.size() - sizeof(int32_t) * 2);
      right_index_ = DecodeFixed32(internal_value_slice.data() +
            internal_value_slice.size() - sizeof(int32_t));
    }
    count_ = DecodeFixed32(internal_value_slice.data());
  }

  virtual void StripSuffix() override {
    if (value_ != nullptr) {
      value_->erase(value_->size() - kListsMetaValueSuffixLength,
          kListsMetaValueSuffixLength);
    }
  }

  virtual void SetVersionToValue() override {
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() -
        kListsMetaValueSuffixLength;
      EncodeFixed32(dst, version_);
    }
  }

  virtual void SetTimestampToValue() override {
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() -
        3 * sizeof(int32_t);
      EncodeFixed32(dst, timestamp_);
    }
  }

  void SetIndexToValue() {
    if (value_ != nullptr) {
      char *dst = const_cast<char *>(value_->data()) + value_->size() -
        2 * sizeof(int32_t);
      EncodeFixed32(dst,  left_index_);
      dst += sizeof(int32_t);
      EncodeFixed32(dst, right_index_);
    }
  }

  static const size_t kListsMetaValueSuffixLength = 4 * sizeof(int32_t);

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

  uint32_t left_index() {
    return left_index_;
  }

  void set_left_index(uint32_t index) {
    left_index_ = index;
    if (value_ != nullptr) {
      char* dst = const_cast<char *>(value_->data()) + value_->size() -
        2 * sizeof(int32_t);
      EncodeFixed32(dst, left_index_);
    }
  }

  void ModifyLeftIndex(uint32_t index) {
    left_index_ -= index;
    if (value_ != nullptr) {
      char* dst = const_cast<char *>(value_->data()) + value_->size() -
        2 * sizeof(int32_t);
      EncodeFixed32(dst, left_index_);
    }
  }

  uint32_t right_index() {
    return right_index_;
  }

  void set_right_index(uint32_t index) {
    right_index_ = index;
    if (value_ != nullptr) {
      char* dst = const_cast<char *>(value_->data()) + value_->size() -
        sizeof(int32_t);
      EncodeFixed32(dst, right_index_);
    }
  }

  void ModifyRightIndex(uint32_t index) {
    right_index_ += index;
    if (value_ != nullptr) {
      char* dst = const_cast<char *>(value_->data()) + value_->size() -
        sizeof(int32_t);
      EncodeFixed32(dst, right_index_);
    }
  }

 private:
  int32_t count_;
  uint32_t left_index_ = 2147483647;
  uint32_t right_index_ = 2147483648;
};

}  //  namespace blackwidow
#endif  //  SRC_LISTS_META_VALUE_FORMAT_H_

