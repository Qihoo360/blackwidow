//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_CUSTOM_COMPARATOR_H_
#define INCLUDE_CUSTOM_COMPARATOR_H_
#include "string"

#include "src/coding.h"

namespace blackwidow {

class ListsDataKeyComparatorImpl : public rocksdb::Comparator {
 public:
  ListsDataKeyComparatorImpl() { }

  const char* Name() const override {
    return "blackwidow.ListsDataKeyComparator";
  }

  int Compare(const Slice& a, const Slice& b) const override {
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

    uint64_t index_a = DecodeFixed64(ptr_a);
    uint64_t index_b = DecodeFixed64(ptr_b);
    ptr_a += sizeof(uint64_t);
    ptr_b += sizeof(uint64_t);
    if (index_a != index_b) {
      return index_a < index_b ? -1 : 1;
    } else {
      return 0;
    }
  }

  bool Equal(const Slice& a, const Slice& b) const override {
    return !Compare(a, b);
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
  }

  void FindShortSuccessor(std::string* key) const override {
  }
};

class ZSetsScoreKeyComparatorImpl : public rocksdb::Comparator {
 public:
  const char* Name() const override {
    return "blackwidow.ZSetsScoreKeyComparator";
  }

  int Compare(const Slice& a, const Slice& b) const override {
    assert(a.size() > sizeof(int32_t));
    assert(a.size() >= DecodeFixed32(a.data())
            + 2 * sizeof(int32_t) + sizeof(uint64_t));
    assert(b.size() > sizeof(int32_t));
    assert(b.size() >= DecodeFixed32(b.data())
            + 2 * sizeof(int32_t) + sizeof(uint64_t));

    const char* ptr_a = a.data();
    const char* ptr_b = b.data();
    int32_t a_size = static_cast<int32_t>(a.size());
    int32_t b_size = static_cast<int32_t>(b.size());
    int32_t key_a_len = DecodeFixed32(ptr_a);
    int32_t key_b_len = DecodeFixed32(ptr_b);
    Slice key_a_prefix(ptr_a,  key_a_len + 2 * sizeof(int32_t));
    Slice key_b_prefix(ptr_b,  key_b_len + 2 * sizeof(int32_t));
    ptr_a += key_a_len + 2 * sizeof(int32_t);
    ptr_b += key_b_len + 2 * sizeof(int32_t);
    int ret = key_a_prefix.compare(key_b_prefix);
    if (ret) {
      return ret;
    }

    uint64_t a_i = DecodeFixed64(ptr_a);
    uint64_t b_i = DecodeFixed64(ptr_b);
    const void* ptr_a_score = reinterpret_cast<const void*>(&a_i);
    const void* ptr_b_score = reinterpret_cast<const void*>(&b_i);
    double a_score = *reinterpret_cast<const double*>(ptr_a_score);
    double b_score = *reinterpret_cast<const double*>(ptr_b_score);
    ptr_a += sizeof(uint64_t);
    ptr_b += sizeof(uint64_t);
    if (a_score != b_score) {
      return a_score < b_score  ? -1 : 1;
    } else {
      if (ptr_a - a.data() == a_size && ptr_b - b.data() == b_size) {
        return 0;
      } else if (ptr_a - a.data() == a_size) {
        return -1;
      } else if (ptr_b - b.data() == b_size) {
        return 1;
      } else {
        Slice key_a_member(ptr_a, a_size - (ptr_a - a.data()));
        Slice key_b_member(ptr_b, b_size - (ptr_b - b.data()));
        ret = key_a_member.compare(key_b_member);
        if (ret) {
          return ret;
        }
      }
    }
    return 0;
  }

  bool Equal(const Slice& a, const Slice& b) const override {
    return !Compare(a, b);
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
  }

  void FindShortSuccessor(std::string* key) const override {
  }
};

}  //  namespace blackwidow
#endif  //  INCLUDE_CUSTOM_COMPARATOR_H_
