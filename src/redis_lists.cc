//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_lists.h"

#include <memory>

#include "src/util.h"
#include "src/lists_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"

namespace blackwidow {

RedisLists::~RedisLists() {
  for (auto handle : handles_) {
    delete handle;
  }
}

Status RedisLists::Open(const rocksdb::Options& options,
                        const std::string& db_path) {
  rocksdb::Options ops(options);
  Status s = rocksdb::DB::Open(ops, db_path, &db_);
  if (s.ok()) {
    // Create column family
    rocksdb::ColumnFamilyHandle* cf;
    s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
                                "data_cf", &cf);
    if (!s.ok()) {
      return s;
    }
    // Close DB
    delete cf;
    delete db_;
  }

  // Open
  rocksdb::DBOptions db_ops(options);
  rocksdb::ColumnFamilyOptions meta_cf_ops(options);
  rocksdb::ColumnFamilyOptions data_cf_ops(options);
  meta_cf_ops.compaction_filter_factory =
    std::make_shared<ListsMetaFilterFactory>();
  data_cf_ops.compaction_filter_factory =
    std::make_shared<ListsDataFilterFactory>(&db_, &handles_);
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Meta CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, meta_cf_ops));
  // Data CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      "data_cf", data_cf_ops));
  return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status RedisLists::LPush(const Slice& key,
                         const std::vector<std::string>& values,
                         int32_t* ret) {
  rocksdb::WriteBatch batch;
  int32_t version = 0;
  uint32_t index = 0;
  *ret = 0;
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      version = parsed_meta_value.UpdateVersion();
      for (auto value : values) {
        index = parsed_meta_value.left_index();
        parsed_meta_value.ModifyLeftIndex(1);
        parsed_meta_value.ModifyCount(1);
        parsed_meta_value.set_timestamp(0);
        ListsDataKey data_key(key, version, index);
        batch.Put(handles_[1], data_key.Encode(), value);
      }
      batch.Put(handles_[0], key, meta_value);
      *ret = parsed_meta_value.count();
    } else {
      for (auto value : values) {
        version = parsed_meta_value.version();
        index = parsed_meta_value.left_index();
        parsed_meta_value.ModifyLeftIndex(1);
        parsed_meta_value.ModifyCount(1);
        ListsDataKey data_key(key, version, index);
        batch.Put(handles_[1], data_key.Encode(), value);
      }
      batch.Put(handles_[0], key, meta_value);
      *ret = parsed_meta_value.count();
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, values.size());
    ListsMetaValue lists_meta_value(std::string(str, sizeof(int32_t)));
    version = lists_meta_value.UpdateVersion();
    for (auto value : values) {
      index = lists_meta_value.left_index();
      lists_meta_value.ModifyLeftIndex(1);
      lists_meta_value.set_timestamp(0);
      ListsDataKey data_key(key, version, index);
      batch.Put(handles_[1], data_key.Encode(), value);
    }
    batch.Put(handles_[0], key, lists_meta_value.Encode());
    *ret = lists_meta_value.right_index() - lists_meta_value.left_index() - 1;
  } else {
    return s;
  }

  return db_->Write(default_write_options_, &batch);
}

Status RedisLists::RPush(const Slice& key,
                         const std::vector<std::string>& values,
                         int32_t* ret) {
  rocksdb::WriteBatch batch;
  int32_t version = 0;
  uint32_t index = 0;
  *ret = 0;
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      version = parsed_meta_value.UpdateVersion();
      for (auto value : values) {
        index = parsed_meta_value.right_index();
        parsed_meta_value.ModifyRightIndex(1);
        parsed_meta_value.ModifyCount(1);
        parsed_meta_value.set_timestamp(0);
        ListsDataKey data_key(key, version, index);
        batch.Put(handles_[1], data_key.Encode(), value);
      }
      batch.Put(handles_[0], key, meta_value);
      *ret = parsed_meta_value.count();
    } else {
      for (auto value : values) {
        version = parsed_meta_value.version();
        index = parsed_meta_value.right_index();
        parsed_meta_value.ModifyRightIndex(1);
        parsed_meta_value.ModifyCount(1);
        batch.Put(handles_[0], key, meta_value);
        ListsDataKey data_key(key, version, index);
        batch.Put(handles_[1], data_key.Encode(), value);
      }
      batch.Put(handles_[0], key, meta_value);
      *ret = parsed_meta_value.count();
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, values.size());
    ListsMetaValue lists_meta_value(std::string(str, sizeof(int32_t)));
    version = lists_meta_value.UpdateVersion();
    for (auto value : values) {
      index = lists_meta_value.right_index();
      lists_meta_value.ModifyRightIndex(1);
      lists_meta_value.set_timestamp(0);
      ListsDataKey data_key(key, version, index);
      batch.Put(handles_[1], data_key.Encode(), value);
    }
    batch.Put(handles_[0], key, lists_meta_value.Encode());
    *ret = lists_meta_value.right_index() - lists_meta_value.left_index() - 1;
  } else {
    return s;
  }

  return db_->Write(default_write_options_, &batch);
}

Status RedisLists::LRange(const Slice& key, int32_t start, int32_t stop,
                          std::vector<std::string>* ret) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::string meta_value;

  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      return s;
    } else {
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
      int32_t version = parsed_meta_value.version();
      uint32_t start_index = start >= 0 ?
                             parsed_meta_value.left_index() + start + 1 :
                             parsed_meta_value.right_index() + start;
      uint32_t stop_index = stop >= 0 ?
                            parsed_meta_value.left_index() + stop + 1 :
                            parsed_meta_value.right_index() + stop;
      if (start_index > stop_index) {
        return s;
      }
      if (stop_index >= parsed_meta_value.right_index()) {
        stop_index = parsed_meta_value.right_index() - 1;
      }
      ListsDataKey start_data_key(key, version, start_index);
      for (iter->Seek(start_data_key.Encode());
           iter->Valid() && start_index <= stop_index;
           iter->Next(), start_index++) {
        ret->push_back(iter->value().ToString());
      }
      return s;
    }
  } else {
    return s;
  }
}

Status RedisLists::CompactRange(const rocksdb::Slice* begin,
                                 const rocksdb::Slice* end) {
  Status s = db_->CompactRange(default_compact_range_options_,
      handles_[0], begin, end);
  if (!s.ok()) {
    return s;
  }
  return db_->CompactRange(default_compact_range_options_,
      handles_[1], begin, end);
}

Status RedisLists::Expire(const Slice& key, int32_t ttl) {
  Status s;
  return s;
}

Status RedisLists::Del(const Slice& key) {
  Status s;
  return s;
}

bool RedisLists::Scan(const std::string& start_key,
                      const std::string& pattern,
                      std::vector<std::string>* keys,
                      int64_t* count,
                      std::string* next_key) {
  return true;
}

Status RedisLists::Expireat(const Slice& key, int32_t timestamp) {
  Status s;
  return s;
}

Status RedisLists::Persist(const Slice& key) {
  Status s;
  return s;
}

Status RedisLists::TTL(const Slice& key, int64_t* timestamp) {
  Status s;
  return s;
}

}   //  namespace blackwidow

