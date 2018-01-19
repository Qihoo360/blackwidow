//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_STRINGS_FILTER_H_
#define SRC_STRINGS_FILTER_H_

#include <string>
#include <memory>

#include "src/strings_value_format.h"
#include "rocksdb/compaction_filter.h"

namespace blackwidow {

class StringsFilter : public rocksdb::CompactionFilter {
 public:
  StringsFilter() = default;
  virtual bool Filter(int level, const rocksdb::Slice& key,
                      const rocksdb::Slice& value,
                      std::string* new_value, bool* value_changed) const
      override {
    ParsedStringsValue parsed_strings_value(value);
    return parsed_strings_value.IsStale();
  }

  virtual const char* Name() const override { return "StringsFilter"; }
};

class StringsFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  StringsFilterFactory() = default;
  virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new StringsFilter());
  }
  virtual const char* Name() const override { return "StringsFilterFactory"; }
};

}  //  namespace blackwidow
#endif  // SRC_STRINGS_FILTER_H_
