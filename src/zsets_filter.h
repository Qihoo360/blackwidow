//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_ZSETS_FILTER_h
#define SRC_ZSETS_FILTER_h

#include "rocksdb/compaction_filter.h"

#include "hashes_filter.h"
#include "base_meta_value_format.h"
#include "zsets_data_key_format.h"


namespace blackwidow {

class ZSetsMetaFilter : public HashesMetaFilter {
 public:
  ZSetsMetaFilter() = default;
  virtual const char* Name() const override {
    return "ZSetsMetaFilter";
  }
};

class ZSetsMetaFilterFactory : public HashesMetaFilterFactory {
 public:
  ZSetsMetaFilterFactory() = default;
  virtual const char* Name() const override {
    return "ZSetsMetaFilterFactory";
  }
};

class ZSetsDataFilter : public HashesDataFilter {
 public:
  ZSetsDataFilter(rocksdb::DB* db,
      std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr) :
    HashesDataFilter(db, handles_ptr) {
  }

  virtual const char* Name() const override {
    return "ZSetsDataFilter";
  };
};

class ZSetsDataFilterFactory : public HashesDataFilterFactory {
 public:
  ZSetsDataFilterFactory(rocksdb::DB** db,
      std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr) :
    HashesDataFilterFactory(db, handles_ptr) {
  }
  virtual const char* Name() const override {
    return "ZSetsDataFilterFactory";
  }
};

class ZSetsScoreFilter : public rocksdb::CompactionFilter {
 public:
  ZSetsScoreFilter(rocksdb::DB* db,
      std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr) :
    db_(db), cf_handles_ptr_(handles_ptr), meta_not_found_(false) {
  }

  virtual bool Filter(int level, const rocksdb::Slice& key,
                      const rocksdb::Slice& value,
                      std::string* new_value, bool* value_changed) const
      override {
    ParsedZSetsScoreKey parsed_score_key(key);

    Trace("------------------------------------------------------------------");
    Trace("[ScoreFilter], key: %s, field: %lf, version: %d",
        parsed_score_key.key().ToString().c_str(),
        parsed_score_key.score(),
        parsed_score_key.version());

    if (parsed_score_key.key().ToString() != cur_key_) {
      Trace("+++++++++++++++++++++++++++++++++++++++++++++++++++");
      Trace("!!!===>Prev Key: %s", cur_key_.c_str());
      cur_key_ = parsed_score_key.key().ToString();
      std::string meta_value;
      Status s = db_->Get(default_read_options_, (*cf_handles_ptr_)[0],
          cur_key_, &meta_value);
      if (s.ok()) {
        meta_not_found_ = false;
        ParsedZSetsMetaValue parsed_meta_value(&meta_value);
        cur_meta_version_ = parsed_meta_value.version();
        cur_meta_timestamp_ = parsed_meta_value.timestamp();
        Trace("Update cur_key to "
            "[%s, cur_meta_version: %d, cur_meta_timestamp: %d]",
            cur_key_.c_str(),
            cur_meta_version_, cur_meta_timestamp_);
        Trace("+++++++++++++++++++++++++++++++++++++++++++++++++++");
      } else if (s.IsNotFound()) {
        meta_not_found_ = true;
        Trace("Update cur_key to "
            "[%s, but meta_key not found]",
            cur_key_.c_str());
        Trace("+++++++++++++++++++++++++++++++++++++++++++++++++++");
      } else {
        Trace("Update cur_key to "
            "[%s, but Get meta_key FAILED] | Reserve",
            cur_key_.c_str());
        Trace("+++++++++++++++++++++++++++++++++++++++++++++++++++");
        return false;
      }
    }

    if (meta_not_found_) {
      Trace("Drop[Meta key not exist]");
      return true;
    }

    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (cur_meta_timestamp_ != 0 &&
        cur_meta_timestamp_ < static_cast<int32_t>(unix_time)) {
      Trace("Drop[cur_meta_timestamp < cur_time(%d)]",
          static_cast<int32_t>(unix_time));
      return true;
    }
#ifndef NDEBUG
    if (cur_meta_version_ > parsed_score_key.version()) {
      Trace("Drop[field_version < cur_time_version]");
    } else {
      Trace("Reserve[field_version == cur_meta_version]");
    }
    Trace("------------------------------------------------------------------");
#endif
    return cur_meta_version_ > parsed_score_key.version();
  }

  virtual const char* Name() const override {
    return "ZSetsScoreFilter";
  }
 private:
  rocksdb::DB* db_;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_;
  rocksdb::ReadOptions default_read_options_;
  mutable std::string cur_key_;
  mutable bool meta_not_found_;
  mutable int32_t cur_meta_version_;
  mutable int32_t cur_meta_timestamp_;
};

class ZSetsScoreFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  ZSetsScoreFilterFactory(rocksdb::DB** db_ptr,
      std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr)
    : db_ptr_(db_ptr), cf_handles_ptr_(handles_ptr) {
 }

  virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new ZSetsScoreFilter(*db_ptr_, cf_handles_ptr_));
  }

  virtual const char* Name() const override {
    return "ZSetsScoreFilterFactory";
  }

 private:
   rocksdb::DB** db_ptr_;
   std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_;
};

} //  blackwidow

#endif
