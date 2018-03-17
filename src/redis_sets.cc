//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_sets.h"

#include <map>
#include <memory>
#include <algorithm>

#include "src/util.h"
#include "src/sets_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"

namespace blackwidow {

RedisSets::~RedisSets() {
  for (auto handle : handles_) {
    delete handle;
  }
}

Status RedisSets::Open(const rocksdb::Options& options,
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
      std::make_shared<SetsMetaFilterFactory>();
  member_cf_ops.compaction_filter_factory =
      std::make_shared<SetsMemberFilterFactory>(&db_, &handles_);
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Meta CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, meta_cf_ops));
  // Member CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      "member_cf", member_cf_ops));
  return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status RedisSets::SAdd(const Slice& key,
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
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      version = parsed_sets_meta_value.UpdateVersion();
      parsed_sets_meta_value.set_count(filtered_members.size());
      parsed_sets_meta_value.set_timestamp(0);
      batch.Put(handles_[0], key, meta_value);
      for (const auto& member : filtered_members) {
        SetsMemberKey sets_member_key(key, version, member);
        batch.Put(handles_[1], sets_member_key.Encode(), Slice());
      }
      *ret = filtered_members.size();
    } else {
      int32_t cnt = 0;
      std::string member_value;
      version = parsed_sets_meta_value.version();
      for (const auto& member : filtered_members) {
        SetsMemberKey sets_member_key(key, version, member);
        s = db_->Get(read_options, handles_[1],
                     sets_member_key.Encode(), &member_value);
        if (s.ok()) {
          cnt++;
        } else if (s.IsNotFound()) {
          cnt++;
          batch.Put(handles_[1], sets_member_key.Encode(), Slice());
        } else {
          return s;
        }
      }
      parsed_sets_meta_value.ModifyCount(cnt);
      batch.Put(handles_[0], key, meta_value);
      *ret = cnt;
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, filtered_members.size());
    SetsMetaValue sets_meta_value(std::string(str, sizeof(int32_t)));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, sets_meta_value.Encode());
    for (const auto& member : filtered_members) {
      SetsMemberKey sets_member_key(key, version, member);
      batch.Put(handles_[1], sets_member_key.Encode(), Slice());
    }
    *ret = filtered_members.size();
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisSets::SCard(const Slice& key, int32_t* ret) {
  std::string meta_value;
  *ret = 0;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      *ret = parsed_sets_meta_value.count();
      if (*ret == 0) {
        return Status::NotFound("Deleted");
      }
    }
  }
  return s;
}

Status RedisSets::SDiff(const std::vector<std::string>& keys,
                        std::vector<std::string>* members) {
  if (keys.size() <= 0) {
    return Status::Corruption("SDiff invalid parameter, no keys");
  }

  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<BlackWidow::KeyVersion> vaild_sets;
  Status s;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    s = db_->Get(read_options, handles_[0], keys[idx], &meta_value);
    if (s.ok()) {
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (!parsed_sets_meta_value.IsStale()) {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.version()});
      }
    } else if (!s.IsNotFound()) {
      return s;
    }
  }

  s = db_->Get(read_options, handles_[0], keys[0], &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (!parsed_sets_meta_value.IsStale()) {
      bool found;
      std::string prefix;
      std::string member_value;
      version = parsed_sets_meta_value.version();
      SetsMemberKey::EncodePrefix(keys[0], version, &prefix);
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        Slice member = parsed_sets_member_key.member();

        found = false;
        for (const auto& key_version : vaild_sets) {
          SetsMemberKey sets_member_key(key_version.key,
                  key_version.version, member);
          s = db_->Get(read_options, handles_[1],
                  sets_member_key.Encode(), &member_value);
          if (s.ok()) {
            found = true;
            break;
          } else if (!s.IsNotFound()) {
            delete iter;
            return s;
          }
        }
        if (!found) {
          members->push_back(member.ToString());
        }
      }
      delete iter;
    }
  } else if (!s.IsNotFound()) {
    return s;
  }
  return Status::OK();
}

Status RedisSets::SDiffstore(const Slice& destination,
                             const std::vector<std::string>& keys,
                             int32_t* ret) {
  if (keys.size() <= 0) {
    return Status::Corruption("SDiffsotre invalid parameter, no keys");
  }

  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeRecordLock l(lock_mgr_, destination);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<BlackWidow::KeyVersion> vaild_sets;
  Status s;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    s = db_->Get(read_options, handles_[0], keys[idx], &meta_value);
    if (s.ok()) {
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (!parsed_sets_meta_value.IsStale()) {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.version()});
      }
    } else if (!s.IsNotFound()) {
      return s;
    }
  }

  std::vector<std::string> members;
  s = db_->Get(read_options, handles_[0], keys[0], &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (!parsed_sets_meta_value.IsStale()) {
      bool found;
      std::string prefix;
      std::string member_value;
      version = parsed_sets_meta_value.version();
      SetsMemberKey::EncodePrefix(keys[0], version, &prefix);
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        Slice member = parsed_sets_member_key.member();

        found = false;
        for (const auto& key_version : vaild_sets) {
          SetsMemberKey sets_member_key(key_version.key,
                  key_version.version, member);
          s = db_->Get(read_options, handles_[1],
                  sets_member_key.Encode(), &member_value);
          if (s.ok()) {
            found = true;
            break;
          } else if (!s.IsNotFound()) {
            delete iter;
            return s;
          }
        }
        if (!found) {
          members.push_back(member.ToString());
        }
      }
      delete iter;
    }
  } else if (!s.IsNotFound()) {
    return s;
  }

  s = db_->Get(read_options, handles_[0], destination, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    version = parsed_sets_meta_value.UpdateVersion();
    parsed_sets_meta_value.set_count(members.size());
    parsed_sets_meta_value.set_timestamp(0);
    batch.Put(handles_[0], destination, meta_value);
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, members.size());
    SetsMetaValue sets_meta_value(std::string(str, sizeof(int32_t)));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[0], destination, sets_meta_value.Encode());
  } else {
    return s;
  }
  for (const auto& member : members) {
    SetsMemberKey sets_member_key(destination, version, member);
    batch.Put(handles_[1], sets_member_key.Encode(), Slice());
  }
  *ret = members.size();
  return db_->Write(default_write_options_, &batch);
}

Status RedisSets::SInter(const std::vector<std::string>& keys,
                         std::vector<std::string>* members) {
  if (keys.size() <= 0) {
    return Status::Corruption("SInter invalid parameter, no keys");
  }

  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<BlackWidow::KeyVersion> vaild_sets;
  Status s;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    s = db_->Get(read_options, handles_[0], keys[idx], &meta_value);
    if (s.ok()) {
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (parsed_sets_meta_value.IsStale() ||
        parsed_sets_meta_value.count() == 0) {
        return Status::OK();
      } else {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.version()});
      }
    } else if (s.IsNotFound()) {
      return Status::OK();
    } else {
      return s;
    }
  }

  s = db_->Get(read_options, handles_[0], keys[0], &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale() ||
      parsed_sets_meta_value.count() == 0) {
      return Status::OK();
    } else {
      bool reliable;
      std::string prefix;
      std::string member_value;
      version = parsed_sets_meta_value.version();
      SetsMemberKey::EncodePrefix(keys[0], version, &prefix);
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        Slice member = parsed_sets_member_key.member();

        reliable = true;
        for (const auto& key_version : vaild_sets) {
          SetsMemberKey sets_member_key(key_version.key,
                  key_version.version, member);
          s = db_->Get(read_options, handles_[1],
                  sets_member_key.Encode(), &member_value);
          if (s.ok()) {
            continue;
          } else if (s.IsNotFound()) {
            reliable = false;
            break;
          } else {
            delete iter;
            return s;
          }
        }
        if (reliable) {
          members->push_back(member.ToString());
        }
      }
      delete iter;
    }
  } else if (s.IsNotFound()) {
    return Status::OK();
  } else {
    return s;
  }
  return Status::OK();
}

Status RedisSets::SInterstore(const Slice& destination,
                              const std::vector<std::string>& keys,
                              int32_t* ret) {
  if (keys.size() <= 0) {
    return Status::Corruption("SInterstore invalid parameter, no keys");
  }

  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  bool have_invalid_sets = false;
  ScopeRecordLock l(lock_mgr_, destination);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<BlackWidow::KeyVersion> vaild_sets;
  Status s;

  for (uint32_t idx = 1; idx < keys.size(); ++idx) {
    s = db_->Get(read_options, handles_[0], keys[idx], &meta_value);
    if (s.ok()) {
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (parsed_sets_meta_value.IsStale() ||
        parsed_sets_meta_value.count() == 0) {
        have_invalid_sets = true;
        break;
      } else {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.version()});
      }
    } else if (s.IsNotFound()) {
      have_invalid_sets = true;
      break;
    } else {
      return s;
    }
  }

  std::vector<std::string> members;
  if (!have_invalid_sets) {
    s = db_->Get(read_options, handles_[0], keys[0], &meta_value);
    if (s.ok()) {
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (parsed_sets_meta_value.IsStale() ||
        parsed_sets_meta_value.count() == 0) {
        have_invalid_sets = true;
      } else {
        bool reliable;
        std::string prefix;
        std::string member_value;
        version = parsed_sets_meta_value.version();
        SetsMemberKey::EncodePrefix(keys[0], version, &prefix);
        auto iter = db_->NewIterator(read_options, handles_[1]);
        for (iter->Seek(prefix);
             iter->Valid() && iter->key().starts_with(prefix);
             iter->Next()) {
          ParsedSetsMemberKey parsed_sets_member_key(iter->key());
          Slice member = parsed_sets_member_key.member();

          reliable = true;
          for (const auto& key_version : vaild_sets) {
            SetsMemberKey sets_member_key(key_version.key,
                    key_version.version, member);
            s = db_->Get(read_options, handles_[1],
                    sets_member_key.Encode(), &member_value);
            if (s.ok()) {
              continue;
            } else if (s.IsNotFound()) {
              reliable = false;
              break;
            } else {
              delete iter;
              return s;
            }
          }
          if (reliable) {
            members.push_back(member.ToString());
          }
        }
        delete iter;
      }
    } else if (s.IsNotFound()) {
    } else {
      return s;
    }
  }

  s = db_->Get(read_options, handles_[0], destination, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    version = parsed_sets_meta_value.UpdateVersion();
    parsed_sets_meta_value.set_count(members.size());
    parsed_sets_meta_value.set_timestamp(0);
    batch.Put(handles_[0], destination, meta_value);
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, members.size());
    SetsMetaValue sets_meta_value(std::string(str, sizeof(int32_t)));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[0], destination, sets_meta_value.Encode());
  } else {
    return s;
  }
  for (const auto& member : members) {
    SetsMemberKey sets_member_key(destination, version, member);
    batch.Put(handles_[1], sets_member_key.Encode(), Slice());
  }
  *ret = members.size();
  return db_->Write(default_write_options_, &batch);
}

Status RedisSets::SIsmember(const Slice& key, const Slice& member,
                            int32_t* ret) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("Stale");
    } else {
      std::string member_value;
      version = parsed_sets_meta_value.version();
      SetsMemberKey sets_member_key(key, version, member);
      s = db_->Get(read_options, handles_[1],
              sets_member_key.Encode(), &member_value);
      *ret = s.ok() ? 1 : 0;
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  }
  return s;
}

Status RedisSets::SMembers(const Slice& key,
                           std::vector<std::string>* members) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      std::string prefix;
      version = parsed_sets_meta_value.version();
      SetsMemberKey::EncodePrefix(key, version, &prefix);
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedSetsMemberKey parsed_sets_member_key(iter->key());
        members->push_back(parsed_sets_member_key.member().ToString());
      }
      delete iter;
    }
  }
  return s;
}

Status RedisSets::SMove(const Slice& source, const Slice& destination,
                        const Slice& member, int32_t* ret) {
  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::vector<std::string> keys {source.ToString(), destination.ToString()};
  std::string meta_value;
  int32_t version = 0;
  MultiScopeRecordLock ml(lock_mgr_, keys);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s = db_->Get(read_options, handles_[0], source, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("Stale");
    } else {
      std::string member_value;
      version = parsed_sets_meta_value.version();
      SetsMemberKey sets_member_key(source, version, member);
      s = db_->Get(read_options, handles_[1],
              sets_member_key.Encode(), &member_value);
      if (s.ok()) {
        *ret = 1;
        parsed_sets_meta_value.ModifyCount(-1);
        batch.Put(handles_[0], source, meta_value);
        batch.Delete(handles_[1], sets_member_key.Encode());
      } else if (s.IsNotFound()) {
        *ret = 0;
        return Status::NotFound();
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
    return Status::NotFound();
  } else {
    return s;
  }

  s = db_->Get(read_options, handles_[0], destination, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      version = parsed_sets_meta_value.UpdateVersion();
      parsed_sets_meta_value.set_count(1);
      parsed_sets_meta_value.set_timestamp(0);
      batch.Put(handles_[0], destination, meta_value);
      SetsMemberKey sets_member_key(destination, version, member);
      batch.Put(handles_[1], sets_member_key.Encode(), Slice());
    } else {
      std::string member_value;
      version = parsed_sets_meta_value.version();
      SetsMemberKey sets_member_key(destination, version, member);
      s = db_->Get(read_options, handles_[1],
              sets_member_key.Encode(), &member_value);
      if (s.IsNotFound()) {
        parsed_sets_meta_value.ModifyCount(1);
        batch.Put(handles_[0], destination, meta_value);
        batch.Put(handles_[1], sets_member_key.Encode(), Slice());
      } else if (!s.ok()) {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, 1);
    SetsMetaValue sets_meta_value(std::string(str, sizeof(int32_t)));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[0], destination, sets_meta_value.Encode());
    SetsMemberKey sets_member_key(destination, version, member);
    batch.Put(handles_[1], sets_member_key.Encode(), Slice());
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisSets::SRem(const Slice& key,
                       const std::vector<std::string>& members,
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
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("stale");
    } else {
      int32_t cnt = 0;
      std::string member_value;
      version = parsed_sets_meta_value.version();
      for (const auto& member : members) {
        SetsMemberKey sets_member_key(key, version, member);
        s = db_->Get(read_options, handles_[1],
                sets_member_key.Encode(), &member_value);
        if (s.ok()) {
          cnt++;
          batch.Delete(handles_[1], sets_member_key.Encode());
        } else if (s.IsNotFound()) {
        } else {
          return s;
        }
      }
      *ret = cnt;
      parsed_sets_meta_value.ModifyCount(-cnt);
      batch.Put(handles_[0], key, meta_value);
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
    return Status::NotFound();
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisSets::SUnion(const std::vector<std::string>& keys,
                         std::vector<std::string>* members) {
  if (keys.size() <= 0) {
    return Status::Corruption("SUnion invalid parameter, no keys");
  }

  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<BlackWidow::KeyVersion> vaild_sets;
  Status s;

  for (uint32_t idx = 0; idx < keys.size(); ++idx) {
    s = db_->Get(read_options, handles_[0], keys[idx], &meta_value);
    if (s.ok()) {
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (!parsed_sets_meta_value.IsStale() &&
        parsed_sets_meta_value.count() != 0) {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.version()});
      }
    } else if (!s.IsNotFound()) {
      return s;
    }
  }

  std::string prefix;
  std::map<std::string, bool> result_flag;
  for (const auto& key_version : vaild_sets) {
    SetsMemberKey::EncodePrefix(key_version.key, key_version.version, &prefix);
    auto iter = db_->NewIterator(read_options, handles_[1]);
    for (iter->Seek(prefix);
         iter->Valid() && iter->key().starts_with(prefix);
         iter->Next()) {
      ParsedSetsMemberKey parsed_sets_member_key(iter->key());
      std::string member = parsed_sets_member_key.member().ToString();
      if (result_flag.find(member) == result_flag.end()) {
        members->push_back(member);
        result_flag[member] = true;
      }
    }
    delete iter;
  }
  return Status::OK();
}

Status RedisSets::SUnionstore(const Slice& destination,
                              const std::vector<std::string>& keys,
                              int32_t* ret) {
  if (keys.size() <= 0) {
    return Status::Corruption("SUnionstore invalid parameter, no keys");
  }

  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeRecordLock l(lock_mgr_, destination);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  std::vector<BlackWidow::KeyVersion> vaild_sets;
  Status s;

  for (uint32_t idx = 0; idx < keys.size(); ++idx) {
    s = db_->Get(read_options, handles_[0], keys[idx], &meta_value);
    if (s.ok()) {
      ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
      if (!parsed_sets_meta_value.IsStale() &&
        parsed_sets_meta_value.count() != 0) {
        vaild_sets.push_back({keys[idx], parsed_sets_meta_value.version()});
      }
    } else if (!s.IsNotFound()) {
      return s;
    }
  }

  std::string prefix;
  std::vector<std::string> members;
  std::map<std::string, bool> result_flag;
  for (const auto& key_version : vaild_sets) {
    SetsMemberKey::EncodePrefix(key_version.key, key_version.version, &prefix);
    auto iter = db_->NewIterator(read_options, handles_[1]);
    for (iter->Seek(prefix);
         iter->Valid() && iter->key().starts_with(prefix);
         iter->Next()) {
      ParsedSetsMemberKey parsed_sets_member_key(iter->key());
      std::string member = parsed_sets_member_key.member().ToString();
      if (result_flag.find(member) == result_flag.end()) {
        members.push_back(member);
        result_flag[member] = true;
      }
    }
    delete iter;
  }

  s = db_->Get(read_options, handles_[0], destination, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    version = parsed_sets_meta_value.UpdateVersion();
    parsed_sets_meta_value.set_count(members.size());
    parsed_sets_meta_value.set_timestamp(0);
    batch.Put(handles_[0], destination, meta_value);
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, members.size());
    SetsMetaValue sets_meta_value(std::string(str, sizeof(int32_t)));
    version = sets_meta_value.UpdateVersion();
    batch.Put(handles_[0], destination, sets_meta_value.Encode());
  } else {
    return s;
  }
  for (const auto& member : members) {
    SetsMemberKey sets_member_key(destination, version, member);
    batch.Put(handles_[1], sets_member_key.Encode(), Slice());
  }
  *ret = members.size();
  return db_->Write(default_write_options_, &batch);
}

Status RedisSets::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    if (ttl > 0) {
      parsed_sets_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    } else {
      parsed_sets_meta_value.set_count(0);
      parsed_sets_meta_value.UpdateVersion();
      parsed_sets_meta_value.set_timestamp(0);
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

Status RedisSets::Del(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      parsed_sets_meta_value.set_count(0);
      parsed_sets_meta_value.UpdateVersion();
      parsed_sets_meta_value.set_timestamp(0);
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

bool RedisSets::Scan(const std::string& start_key,
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
    ParsedSetsMetaValue parsed_meta_value(it->value());
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

Status RedisSets::Expireat(const Slice& key, int32_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      parsed_sets_meta_value.set_timestamp(timestamp);
      return db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

Status RedisSets::Persist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_sets_meta_value(&meta_value);
    if (parsed_sets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      int32_t timestamp = parsed_sets_meta_value.timestamp();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_sets_meta_value.set_timestamp(0);
        return db_->Put(default_write_options_, handles_[0], key, meta_value);
      }
    }
  }
  return s;
}

Status RedisSets::TTL(const Slice& key, int64_t* timestamp) {
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedSetsMetaValue parsed_setes_meta_value(&meta_value);
    if (parsed_setes_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else {
      *timestamp = parsed_setes_meta_value.timestamp();
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


Status RedisSets::CompactRange(const rocksdb::Slice* begin,
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
