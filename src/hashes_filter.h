//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_HASHES_FILTER_H_
#define SRC_HASHES_FILTER_H_

#include <string>
#include <memory>
#include <vector>

#include "src/hashes_meta_value_format.h"
#include "src/hashes_data_key_format.h"
#include "rocksdb/compaction_filter.h"

namespace blackwidow {

class HashesMetaFilter : public rocksdb::CompactionFilter {
 public:
  HashesMetaFilter() = default;
  virtual bool Filter(int level, const rocksdb::Slice& key,
                      const rocksdb::Slice& value,
                      std::string* new_value, bool* value_changed) const
      override {
    ParsedHashesMetaValue parsed_meta_value(value);
    printf("MetaFilter, key: %s, ts: %d, ver: %d\n",
        key.ToString().c_str(), parsed_meta_value.timestamp(),
        parsed_meta_value.version());
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    int32_t cur_time = static_cast<int32_t>(unix_time);
    if (parsed_meta_value.timestamp() != 0 &&
        parsed_meta_value.timestamp() < cur_time &&
        parsed_meta_value.version() < cur_time) {
      printf("Drop[1]\n");
      return true;
    }
    if (parsed_meta_value.count() == 0 &&
        parsed_meta_value.version() < cur_time) {
      printf("Drop[2]\n");
      return true;
    }
    printf("Reserve\n");
    return false;
  }

  virtual const char* Name() const override { return "HashesMetaFilter"; }
};

class HashesMetaFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  HashesMetaFilterFactory() = default;
  virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new HashesMetaFilter());
  }
  virtual const char* Name() const override {
    return "HashesMetaFilterFactory";
  }
};

class HashesDataFilter : public rocksdb::CompactionFilter {
 public:
  HashesDataFilter(rocksdb::DB* db,
      std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr)
      : db_(db), cf_handles_ptr_(handles_ptr), meta_not_found_(false) {
  }
  virtual bool Filter(int level, const rocksdb::Slice& key,
                      const rocksdb::Slice& value,
                      std::string* new_value, bool* value_changed) const
      override {
    ParsedHashesDataKey parsed_data_key(key);
    printf("DataFilter, key: %s, field: %s, ver: %d\n",
        parsed_data_key.key().ToString().c_str(),
        parsed_data_key.field().ToString().c_str(),
        parsed_data_key.version());
    if (parsed_data_key.key().ToString() != cur_key_) {
      printf("prev_key: %s, update, ", cur_key_.c_str());
      cur_key_ = parsed_data_key.key().ToString();
      std::string meta_value;
      Status s = db_->Get(default_read_options_, (*cf_handles_ptr_)[0],
          cur_key_, &meta_value);
      if (s.ok()) {
        meta_not_found_ = false;
        ParsedHashesMetaValue parsed_meta_value(&meta_value);
        cur_meta_version_ = parsed_meta_value.version();
        cur_meta_timestamp_ = parsed_meta_value.timestamp();
        printf("cur_meta_ver: %d, cur_meta_ts: %d, ", cur_meta_version_,
            cur_meta_timestamp_);
        printf("meta_not_found: %d\n", meta_not_found_);
      } else if (s.IsNotFound()) {
        meta_not_found_ = true;
        printf("meta_not_found: %d\n", meta_not_found_);
      } else {
        printf("Reserve[1]\n");
        return false;
      }
    }

    if (meta_not_found_) {
      printf("Drop[1]\n");
      return true;
    }

    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (cur_meta_timestamp_ != 0 &&
        cur_meta_timestamp_ < static_cast<int32_t>(unix_time)) {
      printf("Drop[2]\n");
      return true;
    }
    if (cur_meta_version_ > parsed_data_key.version()) {
      printf("Drop[3]\n");
    } else {
      printf("Reserve[2]\n");
    }
    return cur_meta_version_ > parsed_data_key.version();
  }

  virtual const char* Name() const override { return "HashesDataFilter"; }

 private:
  rocksdb::DB* db_;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_;
  rocksdb::ReadOptions default_read_options_;
  mutable std::string cur_key_;
  mutable bool meta_not_found_;
  mutable int32_t cur_meta_version_;
  mutable int32_t cur_meta_timestamp_;
};

class HashesDataFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  HashesDataFilterFactory(rocksdb::DB** db_ptr,
      std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr)
    : db_ptr_(db_ptr), cf_handles_ptr_(handles_ptr) {
  }
  virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new HashesDataFilter(*db_ptr_, cf_handles_ptr_));
  }
  virtual const char* Name() const override {
    return "HashesDataFilterFactory";
  }

 private:
  rocksdb::DB** db_ptr_;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_;
};

}  //  namespace blackwidow
#endif  // SRC_HASHES_FILTER_H_
