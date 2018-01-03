//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_STRING_FILTER_H_
#define SRC_STRING_FILTER_H_

#include <string>
#include <memory>

#include "src/string_converter.h"
#include "rocksdb/compaction_filter.h"

namespace blackwidow {

class StringFilter : public rocksdb::CompactionFilter {
 public:
  explicit StringFilter(StringConverter* converter)
    : converter_(converter) {
  }
  virtual bool Filter(int level, const rocksdb::Slice& key,
                      const rocksdb::Slice& value,
                      std::string* new_value, bool* value_changed) const
      override {
    return converter_->IsStale(value);
  }

  virtual const char* Name() const override { return "StringFilter"; }
 private:
  StringConverter* converter_;
};

class StringFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  explicit StringFilterFactory(StringConverter* converter)
    : converter_(converter) {
  }
  virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new StringFilter(converter_));
  }
  virtual const char* Name() const override { return "StringFilterFactory"; }
 private:
  StringConverter* converter_;
};

}  //  namespace blackwidow
#endif  // SRC_STRING_FILTER_H_
