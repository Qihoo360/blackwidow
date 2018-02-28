//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_setes.h"

#include <memory>
#include <algorithm>

#include "src/util.h"
#include "src/setes_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"

namespace blackwidow {

RedisSetes::~RedisSetes() {
  for (auto handle : handles_) {
    delete handle;
  }
}

Status RedisSetes::Open(const rocksdb::Options& options,
    const std::string& db_path) {
  rocksdb::Options ops(options);
  Status s = rocksdb::DB::Open(ops, db_path, &db_);
  if (s.ok()) {
    // create column family
    rocksdb::ColumnFamilyHandle* cf;
    s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
        "member_cf", &cf);
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
  rocksdb::ColumnFamilyOptions member_cf_ops(options);
  meta_cf_ops.compaction_filter_factory =
      std::make_shared<SetesMetaFilterFactory>();
  member_cf_ops.compaction_filter_factory =
      std::make_shared<SetesMemberFilterFactory>(&db_, &handles_);
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Meta CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, meta_cf_ops));
  // Member CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      "member_cf", member_cf_ops));
  return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status RedisSetes::SAdd(const Slice& key,
                        const std::vector<std::string>& members, int32_t* ret) {
  std::unordered_set<std::string> unique;
  std::vector<std::string> filtered_members;
  for (auto iter = members.begin(); iter != members.end(); ++iter) {
    if (unique.find(*iter) == unique.end()) {
      unique.insert(*iter);
      filtered_members.push_back(*iter);
    }
  }

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
    ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
    if (parsed_setes_meta_value.IsStale()) {
      version = parsed_setes_meta_value.UpdateVersion();
      parsed_setes_meta_value.set_count(filtered_members.size());
      parsed_setes_meta_value.set_timestamp(0);
      batch.Put(handles_[0], key, meta_value);
      for (const auto& member : filtered_members) {
        SetesMemberKey setes_member_key(key, version, member);
        batch.Put(handles_[1], setes_member_key.Encode(), Slice());
      }
      *ret = filtered_members.size();
    } else {
      int32_t cnt = 0;
      std::string member_value;
      version = parsed_setes_meta_value.version();
      for (const auto& member : filtered_members) {
        SetesMemberKey setes_member_key(key, version, member);
        s = db_->Get(read_options, handles_[1],
                     setes_member_key.Encode(), &member_value);
        if (s.ok()) {
          cnt++;
        } else if (s.IsNotFound()) {
          cnt++;
          batch.Put(handles_[1], setes_member_key.Encode(), Slice());
        } else {
          return s;
        }
      }
      parsed_setes_meta_value.ModifyCount(cnt);
      batch.Put(handles_[0], key, meta_value);
      *ret = cnt;
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, filtered_members.size());
    SetesMetaValue setes_meta_value(std::string(str, sizeof(int32_t)));
    version = setes_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, setes_meta_value.Encode());
    for (const auto& member : filtered_members) {
      SetesMemberKey setes_member_key(key, version, member);
      batch.Put(handles_[1], setes_member_key.Encode(), Slice());
    }
    *ret = filtered_members.size();
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisSetes::SCard(const Slice& key, int32_t* ret) {
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
    if (parsed_setes_meta_value.IsStale()) {
      *ret = 0;
    } else {
      *ret = parsed_setes_meta_value.count();
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  }
  return s;
}

Status RedisSetes::SDiff(const std::vector<std::string>& keys,
                         std::vector<std::string>* members) {

  if (keys.size() <= 0) {
    return Status::Corruption("SDiff invalid parameter, no keys");
  }

  std::string pre_key;
  std::vector<std::string> tmp_keys(keys);
  std::sort(tmp_keys.begin(), tmp_keys.end());

  pre_key.clear();
  if (!tmp_keys.empty() &&
      tmp_keys[0].empty()) {
    lock_mgr_->TryLock(pre_key);
  }

  for (const auto& key : tmp_keys) {
    if (pre_key != key) {
      lock_mgr_->TryLock(key);
      pre_key = key;
    }
  }

  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<BlackWidow::KeyVersion> vaild_setes;
  Status s;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    s = db_->Get(read_options, handles_[0], keys[idx], &meta_value);
    if (s.ok()) {
      ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
      if (!parsed_setes_meta_value.IsStale()) {
        vaild_setes.push_back({keys[idx], parsed_setes_meta_value.version()});
      }
    }
  }

  s = db_->Get(read_options, handles_[0], keys[0], &meta_value);
  if (s.ok()) {
    ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
    if (!parsed_setes_meta_value.IsStale()) {
      bool found;
      std::string prefix;
      std::string member_value;
      version = parsed_setes_meta_value.version();
      SetesMemberKey::EncodePrefix(keys[0], version, &prefix);
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetesMemberKey parsed_setes_member_key(iter->key());
        Slice member = parsed_setes_member_key.member();

        found = false;
        for (const auto& key_version : vaild_setes) {
          SetesMemberKey setes_member_key(key_version.key, key_version.version, member);
          s = db_->Get(read_options, handles_[1],
                  setes_member_key.Encode(), &member_value);
          if (s.ok()) {
            found = true;
            break;
          }
        }
        if (!found) {
          members->push_back(member.ToString());
        }
      }
    }
  }

  pre_key.clear();
  if (!tmp_keys.empty() &&
      tmp_keys[0].empty()) {
    lock_mgr_->UnLock(pre_key);
  }

  for (const auto& key : tmp_keys) {
    if (pre_key != key) {
      lock_mgr_->UnLock(key);
      pre_key = key;
    }
  }
  return Status::OK();
}

Status RedisSetes::SDiffstore(const Slice& destination,
                              const std::vector<std::string>& keys,
                              int32_t* ret) {
  if (keys.size() <= 0) {
    return Status::Corruption("SDiff invalid parameter, no keys");
  }

  std::string pre_key;
  std::vector<std::string> tmp_keys(keys);
  tmp_keys.push_back(destination.ToString());
  std::sort(tmp_keys.begin(), tmp_keys.end());

  pre_key.clear();
  if (!tmp_keys.empty() &&
      tmp_keys[0].empty()) {
    lock_mgr_->TryLock(pre_key);
  }

  for (const auto& key : tmp_keys) {
    if (pre_key != key) {
      lock_mgr_->TryLock(key);
      pre_key = key;
    }
  }

  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<BlackWidow::KeyVersion> vaild_setes;
  Status s;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    s = db_->Get(read_options, handles_[0], keys[idx], &meta_value);
    if (s.ok()) {
      ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
      if (!parsed_setes_meta_value.IsStale()) {
        vaild_setes.push_back({keys[idx], parsed_setes_meta_value.version()});
      }
    }
  }

  std::vector<std::string> members;
  s = db_->Get(read_options, handles_[0], keys[0], &meta_value);
  if (s.ok()) {
    ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
    if (!parsed_setes_meta_value.IsStale()) {
      bool found;
      std::string prefix;
      std::string member_value;
      version = parsed_setes_meta_value.version();
      SetesMemberKey::EncodePrefix(keys[0], version, &prefix);
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetesMemberKey parsed_setes_member_key(iter->key());
        Slice member = parsed_setes_member_key.member();

        found = false;
        for (const auto& key_version : vaild_setes) {
          SetesMemberKey setes_member_key(key_version.key, key_version.version, member);
          s = db_->Get(read_options, handles_[1],
                  setes_member_key.Encode(), &member_value);
          if (s.ok()) {
            found = true;
            break;
          }
        }
        if (!found) {
          members.push_back(member.ToString());
        }
      }
    }
  } else if (s.IsNotFound()) {
  } else {
    return s;
  }

  s = db_->Get(read_options, handles_[0], destination, &meta_value);
  if (s.ok()) {
    ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
    version = parsed_setes_meta_value.UpdateVersion();
    parsed_setes_meta_value.set_count(members.size());
    parsed_setes_meta_value.set_timestamp(0);
    batch.Put(handles_[0], destination, meta_value);
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, members.size());
    SetesMetaValue setes_meta_value(std::string(str, sizeof(int32_t)));
    version = setes_meta_value.UpdateVersion();
    batch.Put(handles_[0], destination, setes_meta_value.Encode());
  } else {
    return s;
  }
  for (const auto& member : members) {
    SetesMemberKey setes_member_key(destination, version, member);
    batch.Put(handles_[1], setes_member_key.Encode(), Slice());
  }
  *ret = members.size();

  // bug
  pre_key.clear();
  if (!tmp_keys.empty() &&
      tmp_keys[0].empty()) {
    lock_mgr_->UnLock(pre_key);
  }

  for (const auto& key : tmp_keys) {
    if (pre_key != key) {
      lock_mgr_->UnLock(key);
      pre_key = key;
    }
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisSetes::SIsmember(const Slice& key, const Slice& member,
                             int32_t* ret) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeRecordLock l(lock_mgr_, key);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
    if (parsed_setes_meta_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("Stale");
    } else {
      std::string member_value;
      version = parsed_setes_meta_value.version();
      SetesMemberKey setes_member_key(key, version, member);
      s = db_->Get(read_options, handles_[1],
              setes_member_key.Encode(), &member_value);
      *ret = s.ok() ? 1 : 0;
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  }
  return s;
}

Status RedisSetes::SMembers(const Slice& key,
                            std::vector<std::string>* members) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeRecordLock l(lock_mgr_, key);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
    if (parsed_setes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      std::string prefix;
      version = parsed_setes_meta_value.version();
      SetesMemberKey::EncodePrefix(key, version, &prefix);
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetesMemberKey parsed_setes_member_key(iter->key());
        members->push_back(parsed_setes_member_key.member().ToString());
      }
    }
  }
  return s;
}

Status RedisSetes::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
    if (parsed_setes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    if (ttl > 0) {
      parsed_setes_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    } else {
      parsed_setes_meta_value.set_count(0);
      parsed_setes_meta_value.UpdateVersion();
      parsed_setes_meta_value.set_timestamp(0);
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

Status RedisSetes::Del(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
    if (parsed_setes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      parsed_setes_meta_value.set_count(0);
      parsed_setes_meta_value.UpdateVersion();
      parsed_setes_meta_value.set_timestamp(0);
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

bool RedisSetes::Scan(const std::string& start_key,
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
    ParsedSetesMetaValue parsed_meta_value(it->value());
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

Status RedisSetes::Expireat(const Slice& key, int32_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
    if (parsed_setes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      parsed_setes_meta_value.set_timestamp(timestamp);
      return db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

Status RedisSetes::Persist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
    if (parsed_setes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      int32_t timestamp = parsed_setes_meta_value.timestamp();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_setes_meta_value.set_timestamp(0);
        return db_->Put(default_write_options_, handles_[0], key, meta_value);
      }
    }
  }
  return s;
}

Status RedisSetes::TTL(const Slice& key, int32_t* timestamp) {
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetesMetaValue parsed_setes_meta_value(&meta_value);
    if (parsed_setes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      *timestamp = parsed_setes_meta_value.timestamp();
    }
  }
  return s;
}


Status RedisSetes::CompactRange(const rocksdb::Slice* begin,
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
