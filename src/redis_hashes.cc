//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_hashes.h"

#include <memory>

#include "src/util.h"
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
      parsed.set_timestamp(0);
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

Status RedisHashes::HMSet(const Slice& key,
                          const std::vector<BlackWidow::SliceFieldValue>& fvs) {
  std::unordered_set<std::string> fields;
  std::vector<BlackWidow::SliceFieldValue> filtered_fvs;
  for (auto iter = fvs.rbegin(); iter != fvs.rend(); ++iter) {
    std::string field = iter->field.ToString();
    if (fields.find(field) == fields.end()) {
      fields.insert(field);
      filtered_fvs.push_back(*iter);
    }
  }

  int32_t version = 0;
  std::string meta_value;
  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeRecordLock l(lock_mgr_, key);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      version = parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_count(filtered_fvs.size());
      parsed_hashes_meta_value.set_timestamp(0);
      batch.Put(handles_[0], key, meta_value);
      for (const auto& fv : filtered_fvs) {
        HashesDataKey hashes_data_key(key, version, fv.field);
        batch.Put(handles_[1], hashes_data_key.Encode(), fv.value);
      }
    } else {
      int32_t count = 0;
      std::string data_value;
      version = parsed_hashes_meta_value.version();
      for (const auto& fv : filtered_fvs) {
        HashesDataKey hashes_data_key(key, version, fv.field);
        s = db_->Get(read_options, handles_[1],
                hashes_data_key.Encode(), &data_value);
        if (s.ok()) {
          batch.Put(handles_[1], hashes_data_key.Encode(), fv.value);
        } else if (s.IsNotFound()) {
          count++;
          batch.Put(handles_[1], hashes_data_key.Encode(), fv.value);
        } else {
          return s;
        }
      }
      parsed_hashes_meta_value.ModifyCount(count);
      batch.Put(handles_[0], key, meta_value);
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, filtered_fvs.size());
    HashesMetaValue hashes_meta_value(std::string(str, sizeof(int32_t)));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, hashes_meta_value.Encode());
    for (const auto& fv : filtered_fvs) {
      HashesDataKey hashes_data_key(key, version, fv.field);
      batch.Put(handles_[1], hashes_data_key.Encode(), fv.value);
    }
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisHashes::HMGet(const Slice& key,
                          const std::vector<Slice>& fields,
                          std::vector<std::string>* values) {
  int32_t version = 0;
  std::string value;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      version = parsed_hashes_meta_value.version();
      for (const auto& field : fields) {
        HashesDataKey hashes_data_key(key, version, field);
        s = db_->Get(read_options, handles_[1],
                hashes_data_key.Encode(), &value);
        if (s.ok()) {
          values->push_back(value);
        } else if (s.IsNotFound()) {
          values->push_back("");
        } else {
          return s;
        }
      }
    }
  } else if (s.IsNotFound()) {
    return Status::NotFound();
  }
  return Status::OK();
}

Status RedisHashes::HSetnx(const Slice& key, const Slice& field, const Slice& value,
                           int32_t* ret) {
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
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      version = parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_count(1);
      parsed_hashes_meta_value.set_timestamp(0);
      batch.Put(handles_[0], key, meta_value);
      HashesDataKey hashes_data_key(key, version, field);
      batch.Put(handles_[1], hashes_data_key.Encode(), value);
      *ret = 1;
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, field);
      std::string data_value;
      s = db_->Get(read_options, handles_[1], hashes_data_key.Encode(), &data_value);
      if (s.ok()) {
        *ret = 0;
      } else if (s.IsNotFound()) {
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[0], key, meta_value);
        batch.Put(handles_[1], hashes_data_key.Encode(), value);
        *ret = 1;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, 1);
    HashesMetaValue hashes_meta_value(std::string(str, sizeof(int32_t)));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, hashes_meta_value.Encode());
    HashesDataKey hashes_data_key(key, version, field);
    batch.Put(handles_[1], hashes_data_key.Encode(), value);
    *ret = 1;
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisHashes::HLen(const Slice& key, int32_t* ret) {
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      *ret = 0;
    } else {
      *ret = parsed_hashes_meta_value.count();
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  }
  return s;
}

Status RedisHashes::HStrlen(const Slice& key, const Slice& field, int32_t* len) {
  std::string value;
  Status s = HGet(key, field, &value);
  if (s.ok()) {
    *len = value.size();
  } else {
    *len = 0;
  }
  return s;
}

Status RedisHashes::HExists(const Slice& key, const Slice& field) {
  std::string value;
  return HGet(key, field, &value);
}

Status RedisHashes::HIncrby(const Slice& key, const Slice& field, int64_t value,
                            int64_t* ret) {
  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  int32_t version = 0;
  std::string old_value;
  std::string meta_value;

  ScopeRecordLock l(lock_mgr_, key);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);

  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      version = parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_count(1);
      parsed_hashes_meta_value.set_timestamp(0);
      batch.Put(handles_[0], key, meta_value);
      HashesDataKey hashes_data_key(key, version, field);
      char buf[32];
      Int64ToStr(buf, 32, value);
      batch.Put(handles_[1], hashes_data_key.Encode(), buf);
      *ret = value;
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(read_options, handles_[1],
              hashes_data_key.Encode(), &old_value);
      if (s.ok()) {
        char* end = nullptr;
        int64_t ival = strtoll(old_value.c_str(), &end, 10);
        if (*end != 0) {
          return Status::InvalidArgument("hash value is not an integer");
        }
        if ((value >= 0 && LLONG_MAX - value < ival) ||
          (value < 0 && LLONG_MIN - value > ival)) {
          return Status::InvalidArgument("Overflow");
        }
        *ret = ival + value;
        char buf[32];
        Int64ToStr(buf, 32, *ret);
        batch.Put(handles_[1], hashes_data_key.Encode(), buf);
      } else if (s.IsNotFound()) {
        char buf[32];
        Int64ToStr(buf, 32, value);
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[0], key, meta_value);
        batch.Put(handles_[1], hashes_data_key.Encode(), buf);
        *ret = value;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, 1);
    HashesMetaValue hashes_meta_value(std::string(str, sizeof(int32_t)));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, hashes_meta_value.Encode());
    HashesDataKey hashes_data_key(key, version, field);

    char buf[32];
    Int64ToStr(buf, 32, value);
    batch.Put(handles_[1], hashes_data_key.Encode(), buf);
    *ret = value;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisHashes::HDel(const Slice& key, const std::vector<Slice>& fields,
                         int32_t* ret) {
  std::vector<Slice> filtered_fields;
  std::unordered_set<std::string> field_set;
  for (auto iter = fields.begin(); iter != fields.end(); ++iter) {
    std::string field = iter->ToString();
    if (field_set.find(field) == field_set.end()) {
      field_set.insert(field);
      filtered_fields.push_back(*iter);
    }
  }

  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t del_cnt = 0;
  int32_t version = 0;
  ScopeRecordLock l(lock_mgr_, key);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      *ret = 0;
      return Status::OK();
    } else {
      std::string data_value;
      version = parsed_hashes_meta_value.version();
      int32_t hlen = parsed_hashes_meta_value.count();
      for (const auto& field : filtered_fields) {
        HashesDataKey hashes_data_key(key, version, field);
        s = db_->Get(read_options, handles_[1], hashes_data_key.Encode(), &data_value);
        if (s.ok()) {
          del_cnt++;
          batch.Delete(handles_[1], hashes_data_key.Encode());
        } else if(s.IsNotFound()) {
          continue;
        } else {
          return s;
        }
      }
      *ret = del_cnt;
      hlen -= del_cnt;
      parsed_hashes_meta_value.set_count(hlen);
      batch.Put(handles_[0], key, meta_value);
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
    return Status::OK();
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
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
