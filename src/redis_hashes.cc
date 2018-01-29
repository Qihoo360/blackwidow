//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_hashes.h"

#include <memory>

#include "src/hashes_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"

namespace blackwidow {

RedisHashes::~RedisHashes() {
  for (auto handle : handles_) {
    delete handle;
  }
}

Status RedisHashes::Open(const rocksdb::Options& options,
    const std::string& db_path) {
  rocksdb::Options ops(options);
  Status s = rocksdb::DB::Open(ops, db_path, &db_);
  if (s.ok()) {
    // create column family
    rocksdb::ColumnFamilyHandle* cf;
    s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
        "data_cf", &cf);
    if (!s.ok()) {
      return s;
    }
    // close DB
    delete cf;
    delete db_;
  }

  // Open
  rocksdb::DBOptions db_ops(options);
  rocksdb::ColumnFamilyOptions meta_cf_ops(options);
  rocksdb::ColumnFamilyOptions data_cf_ops(options);
  meta_cf_ops.compaction_filter_factory =
    std::make_shared<HashesMetaFilterFactory>();
  data_cf_ops.compaction_filter_factory =
    std::make_shared<HashesDataFilterFactory>(&db_, &handles_);
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Meta CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, meta_cf_ops));
  // Data CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      "data_cf", data_cf_ops));
  return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status RedisHashes::HSet(const Slice& key, const Slice& field,
        const Slice& value, int32_t* res) {
  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeRecordLock l(lock_mgr_, key);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed(&meta_value);
    if (parsed.IsStale()) {
      version = parsed.UpdateVersion();
      parsed.set_count(1);
      batch.Put(handles_[0], key, meta_value);
      HashesDataKey data_key(key, version, field);
      batch.Put(handles_[1], data_key.Encode(), value);
      *res = 1;
    } else {
      version = parsed.version();
      HashesDataKey data_key(key, version, field);
      std::string data_value;
      s = db_->Get(read_options, handles_[1], data_key.Encode(), &data_value);
      if (s.ok()) {
        batch.Put(handles_[1], data_key.Encode(), value);
        *res = 0;
      } else if (s.IsNotFound()) {
        parsed.ModifyCount(1);
        batch.Put(handles_[0], key, meta_value);
        batch.Put(handles_[1], data_key.Encode(), value);
        *res = 1;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, 1);
    HashesMetaValue meta_value(std::string(str, sizeof(int32_t)));
    version = meta_value.UpdateVersion();
    batch.Put(handles_[0], key, meta_value.Encode());
    HashesDataKey data_key(key, version, field);
    batch.Put(handles_[1], data_key.Encode(), value);
    *res = 1;
  } else {
    return s;
  }

  return db_->Write(default_write_options_, &batch);
}

Status RedisHashes::HGet(const Slice& key, const Slice& field,
    std::string* value) {
  std::string meta_value;
  int32_t version = 0;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed(&meta_value);
    if (parsed.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      version = parsed.version();
      HashesDataKey data_key(key, version, field);
      s = db_->Get(read_options, handles_[1], data_key.Encode(), value);
    }
  } else if (s.IsNotFound()) {
    return Status::NotFound();
  }

  return s;
}

Status RedisHashes::HExists(const Slice& key, const Slice& field) {
  std::string value;
  return HGet(key, field, &value);
}

Status RedisHashes::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed(&meta_value);
    if (parsed.IsStale()) {
      return Status::NotFound("Stale");
    }
    if (ttl > 0) {
      parsed.SetRelativeTimestamp(ttl);
      return db_->Put(default_write_options_, handles_[0], key, meta_value);
    } else {
      return db_->Delete(default_write_options_, handles_[0], key);
    }
  }
  return s;
}

Status RedisHashes::Del(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed(&meta_value);
    parsed.set_count(0);
    parsed.UpdateVersion();
    s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    if (s.ok() && parsed.IsStale()) {
      return Status::NotFound("Stale");
    }
  }
  return s;
}

Status RedisHashes::CompactRange(const rocksdb::Slice* begin,
    const rocksdb::Slice* end) {
  Status s = db_->CompactRange(default_compact_range_options_,
      handles_[0], begin, end);
  if (!s.ok()) {
    return s;
  }
  return db_->CompactRange(default_compact_range_options_,
      handles_[1], begin, end);
}

}  //  namespace blackwidow
