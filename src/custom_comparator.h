//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_CUSTOM_COMPARATOR_H_
#define INCLUDE_CUSTOM_COMPARATOR_H_
#include "src/coding.h"

namespace blackwidow {

class SetsMemberKeyComparatorImpl : public rocksdb::Comparator {
 public:
  SetsMemberKeyComparatorImpl() { }

  virtual const char* Name() const override {
    return "blackwidow.SetsMemberKeyComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const override {
    assert(!a.empty() && !b.empty());
    const char* ptr_a = a.data();
    const char* ptr_b = b.data();
    int32_t a_size = static_cast<int32_t>(a.size());
    int32_t b_size = static_cast<int32_t>(b.size());
    int32_t key_a_len = DecodeFixed32(ptr_a);
    int32_t key_b_len = DecodeFixed32(ptr_b);
    ptr_a += sizeof(int32_t);
    ptr_b += sizeof(int32_t);
    Slice sets_key_a(ptr_a,  key_a_len);
    Slice sets_key_b(ptr_b,  key_b_len);
    ptr_a += key_a_len;
    ptr_b += key_b_len;
    if (sets_key_a != sets_key_b) {
      return sets_key_a.compare(sets_key_b);
    }
    if (ptr_a - a.data() == a_size &&
      ptr_b - b.data() == b_size) {
      return 0;
    } else if (ptr_a - a.data() == a_size) {
      return -1;
    } else if (ptr_b - b.data() == b_size) {
      return 1;
    }

    int32_t version_a = DecodeFixed32(ptr_a);
    int32_t version_b = DecodeFixed32(ptr_b);
    ptr_a += sizeof(int32_t);
    ptr_b += sizeof(int32_t);
    if (version_a != version_b) {
      return version_a < version_b ? -1 : 1;
    }
    if (ptr_a - a.data() == a_size &&
      ptr_b - b.data() == b_size) {
      return 0;
    } else if (ptr_a - a.data() == a_size) {
      return -1;
    } else if (ptr_b - b.data() == b_size) {
      return 1;
    }

    uint32_t serial_num_a = DecodeFixed32(ptr_a);
    uint32_t serial_num_b = DecodeFixed32(ptr_b);
    ptr_a += sizeof(int32_t);
    ptr_b += sizeof(int32_t);
    if (serial_num_a != serial_num_b) {
      return serial_num_a < serial_num_b ? -1 : 1;
    }
    if (ptr_a - a.data() == a_size &&
      ptr_b - b.data() == b_size) {
      return 0;
    } else if (ptr_a - a.data() == a_size) {
      return -1;
    } else if (ptr_b - b.data() == b_size) {
      return 1;
    }

    Slice sets_member_a(ptr_a, a.size() - key_a_len - sizeof(int32_t) * 3);
    Slice sets_member_b(ptr_b, b.size() - key_b_len - sizeof(int32_t) * 3);
    return sets_member_a.compare(sets_member_b);
  }

  virtual bool Equal(const Slice& a, const Slice& b) const override {
    return a == b;
  }

  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const override {
    assert(Compare(*start, limit) < 0);
  }

  virtual void FindShortSuccessor(std::string* key) const override {
  }
};

}  //  namespace blackwidow
#endif  //  INCLUDE_CUSTOM_COMPARATOR_H_
