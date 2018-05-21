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

Status RedisHashes::HDel(const Slice& key,
                         const std::vector<std::string>& fields,
                         int32_t* ret) {
  std::vector<std::string> filtered_fields;
  std::unordered_set<std::string> field_set;
  for (auto iter = fields.begin(); iter != fields.end(); ++iter) {
    std::string field = *iter;
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
        s = db_->Get(read_options, handles_[1],
                hashes_data_key.Encode(), &data_value);
        if (s.ok()) {
          del_cnt++;
          batch.Delete(handles_[1], hashes_data_key.Encode());
        } else if (s.IsNotFound()) {
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

Status RedisHashes::HExists(const Slice& key, const Slice& field) {
  std::string value;
  return HGet(key, field, &value);
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
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey data_key(key, version, field);
      s = db_->Get(read_options, handles_[1], data_key.Encode(), value);
    }
  }
  return s;
}

Status RedisHashes::HGetall(const Slice& key,
                            std::vector<BlackWidow::FieldValue>* fvs) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, "");
      Slice prefix = hashes_data_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        fvs->push_back({parsed_hashes_data_key.field().ToString(),
                iter->value().ToString()});
      }
      delete iter;
    }
  }
  return s;
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
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisHashes::HIncrbyfloat(const Slice& key, const Slice& field,
                                 const Slice& by, std::string* new_value) {
  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  int32_t version = 0;
  std::string meta_value;
  std::string old_value_str;
  long double long_double_by;

  if (StrToLongDouble(by.data(), by.size(), &long_double_by) == -1) {
    return Status::InvalidArgument("Value is not a vaild float");
  }

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

      LongDoubleToStr(long_double_by, new_value);
      batch.Put(handles_[1], hashes_data_key.Encode(), *new_value);
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(read_options, handles_[1],
              hashes_data_key.Encode(), &old_value_str);
      if (s.ok()) {
        long double total;
        long double old_value;
        if (StrToLongDouble(old_value_str.data(),
                    old_value_str.size(), &old_value) == -1) {
          return Status::InvalidArgument("Hash value is not a vaild float");
        }

        total = old_value + long_double_by;
        if (LongDoubleToStr(total, new_value) == -1) {
          return Status::InvalidArgument("Overflow");
        }
        batch.Put(handles_[1], hashes_data_key.Encode(), *new_value);
      } else if (s.IsNotFound()) {
        LongDoubleToStr(long_double_by, new_value);
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[0], key, meta_value);
        batch.Put(handles_[1], hashes_data_key.Encode(), *new_value);
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
    LongDoubleToStr(long_double_by, new_value);
    batch.Put(handles_[1], hashes_data_key.Encode(), *new_value);
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisHashes::HKeys(const Slice& key,
                          std::vector<std::string>* fields) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, "");
      Slice prefix = hashes_data_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        fields->push_back(parsed_hashes_data_key.field().ToString());
      }
      delete iter;
    }
  }
  return s;
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

Status RedisHashes::HMGet(const Slice& key,
                          const std::vector<std::string>& fields,
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

Status RedisHashes::HMSet(const Slice& key,
                          const std::vector<BlackWidow::FieldValue>& fvs) {
  std::unordered_set<std::string> fields;
  std::vector<BlackWidow::FieldValue> filtered_fvs;
  for (auto iter = fvs.rbegin(); iter != fvs.rend(); ++iter) {
    std::string field = iter->field;
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

Status RedisHashes::HSetnx(const Slice& key, const Slice& field,
                           const Slice& value, int32_t* ret) {
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
      s = db_->Get(read_options, handles_[1],
              hashes_data_key.Encode(), &data_value);
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

Status RedisHashes::HVals(const Slice& key,
                          std::vector<std::string>* values) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, "");
      Slice prefix = hashes_data_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        values->push_back(iter->value().ToString());
      }
      delete iter;
    }
  }
  return s;
}

Status RedisHashes::HStrlen(const Slice& key,
                            const Slice& field, int32_t* len) {
  std::string value;
  Status s = HGet(key, field, &value);
  if (s.ok()) {
    *len = value.size();
  } else {
    *len = 0;
  }
  return s;
}

Status RedisHashes::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    if (ttl > 0) {
      parsed_hashes_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    } else {
      parsed_hashes_meta_value.set_count(0);
      parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_timestamp(0);
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

Status RedisHashes::Del(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      parsed_hashes_meta_value.set_count(0);
      parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_timestamp(0);
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

bool RedisHashes::Scan(const std::string& start_key,
                       const std::string& pattern,
                       std::vector<std::string>* keys,
                       int64_t* count,
                       std::string* next_key) {
  std::string meta_key, meta_value;
  bool is_finish = true;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);

  it->Seek(start_key);
  while (it->Valid() && (*count) > 0) {
    ParsedHashesMetaValue parsed_meta_value(it->value());
    if (parsed_meta_value.IsStale()) {
      it->Next();
      continue;
    } else {
      meta_key = it->key().ToString();
      meta_value = it->value().ToString();
      if (StringMatch(pattern.data(), pattern.size(),
                         meta_key.data(), meta_key.size(), 0)) {
        keys->push_back(meta_key);
      }
      (*count)--;
      it->Next();
    }
  }

  if (it->Valid()) {
    *next_key = it->key().ToString();
    is_finish = false;
  } else {
    *next_key = "";
  }
  delete it;
  return is_finish;
}

Status RedisHashes::Expireat(const Slice& key, int32_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      parsed_hashes_meta_value.set_timestamp(timestamp);
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

Status RedisHashes::Persist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      int32_t timestamp = parsed_hashes_meta_value.timestamp();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      }  else {
        parsed_hashes_meta_value.set_timestamp(0);
        s = db_->Put(default_write_options_, handles_[0], key, meta_value);
      }
    }
  }
  return s;
}

Status RedisHashes::TTL(const Slice& key, int64_t* timestamp) {
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else {
      *timestamp = parsed_hashes_meta_value.timestamp();
      if (*timestamp == 0) {
        *timestamp = -1;
      } else {
        int64_t curtime;
        rocksdb::Env::Default()->GetCurrentTime(&curtime);
        *timestamp = *timestamp - curtime > 0 ? *timestamp - curtime : -1;
      }
    }
  } else if (s.IsNotFound()) {
    *timestamp = -2;
  }
  return s;
}

}  //  namespace blackwidow
