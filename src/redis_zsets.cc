//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_zsets.h"

#include <limits>

#include "iostream"
#include "src/util.h"
#include "src/zsets_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"

namespace blackwidow {

rocksdb::Comparator* ZSetsScoreKeyComparator() {
  static ZSetsScoreKeyComparatorImpl zsets_score_key_compare;
  return &zsets_score_key_compare;
}

RedisZSets::~RedisZSets() {
  for (auto handle : handles_) {
    delete handle;
  }
}

Status RedisZSets::Open(const rocksdb::Options& options,
    const std::string& db_path) {
  rocksdb::Options ops(options);
  Status s = rocksdb::DB::Open(ops, db_path, &db_);
  if (s.ok()) {
    rocksdb::ColumnFamilyHandle *dcf = nullptr, *scf = nullptr;
    s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "data_cf", &dcf);
    if (!s.ok()) {
      return s;
    }
    rocksdb::ColumnFamilyOptions score_cf_ops;
    score_cf_ops.comparator = ZSetsScoreKeyComparator();
    s = db_->CreateColumnFamily(score_cf_ops, "score_cf", &scf);
    if (!s.ok()) {
      return s;
    }
    delete scf;
    delete dcf;
    delete db_;
  }

  rocksdb::DBOptions db_ops(options);
  rocksdb::ColumnFamilyOptions meta_cf_ops(options);
  rocksdb::ColumnFamilyOptions data_cf_ops(options);
  rocksdb::ColumnFamilyOptions score_cf_ops(options);
  meta_cf_ops.compaction_filter_factory =
    std::make_shared<ZSetsMetaFilterFactory>();
  data_cf_ops.compaction_filter_factory =
    std::make_shared<ZSetsDataFilterFactory>(&db_, &handles_);
  score_cf_ops.compaction_filter_factory =
    std::make_shared<ZSetsScoreFilterFactory>(&db_, &handles_);
  score_cf_ops.comparator = ZSetsScoreKeyComparator();

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        rocksdb::kDefaultColumnFamilyName, meta_cf_ops));
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        "data_cf", data_cf_ops));
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        "score_cf", score_cf_ops));
  return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status RedisZSets::CompactRange(const rocksdb::Slice* begin,
    const rocksdb::Slice* end) {
  //TODO: to be implemented
  return Status();
}

Status RedisZSets::ZAdd(const Slice& key,
                        const std::vector<BlackWidow::ScoreMember>& score_members,
                        int32_t* ret) {
  *ret = 0;
  std::unordered_set<std::string> unique;
  std::vector<BlackWidow::ScoreMember> filtered_score_members;
  for (const auto& sm : score_members) {
    if (unique.find(sm.member) == unique.end()) {
      unique.insert(sm.member);
      filtered_score_members.push_back(sm);
    }
  }

  char score_buf[8];
  int32_t version = 0;
  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    bool is_stale = false;
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      is_stale = true;
      version = parsed_zsets_meta_value.InitialMetaValue();
    } else {
      is_stale = false;
      version = parsed_zsets_meta_value.version();
    }

    int32_t new_add = 0;
    int32_t old_size = parsed_zsets_meta_value.count();
    std::string data_value;
    for (const auto& sm : filtered_score_members) {
      bool not_found = true;
      ZSetsDataKey zsets_data_key(key, version, sm.member);
      if (!is_stale) {
        s = db_->Get(default_read_options_, handles_[1], zsets_data_key.Encode(), &data_value);
        if (s.ok()) {
          not_found = false;
          uint64_t tmp = DecodeFixed64(data_value.data());
          const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
          double old_score = *reinterpret_cast<const double*>(ptr_tmp);
          if (old_score == sm.score) {
            continue;
          } else {
            ZSetsScoreKey zsets_score_key(key, version, old_score, sm.member);
            batch.Delete(handles_[2], zsets_score_key.Encode());
          }
        } else if (!s.IsNotFound()) {
          return s;
        }
      }

      const void* ptr_score = reinterpret_cast<const void*>(&sm.score);
      EncodeFixed64(score_buf, *reinterpret_cast<const uint64_t*>(ptr_score));
      batch.Put(handles_[1], zsets_data_key.Encode(), Slice(score_buf, sizeof(uint64_t)));

      ZSetsScoreKey zsets_score_key(key, version, sm.score, sm.member);
      batch.Put(handles_[2], zsets_score_key.Encode(), Slice());
      if (not_found) {
        new_add++;
      }
    }
    parsed_zsets_meta_value.set_count(old_size + new_add);
    batch.Put(handles_[0], key, meta_value);
    *ret = old_size + new_add;
  } else if (s.IsNotFound()) {
    char buf[8];
    EncodeFixed32(buf, filtered_score_members.size());
    ZSetsMetaValue zsets_meta_value(Slice(buf, sizeof(int32_t)));
    version = zsets_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, zsets_meta_value.Encode());
    for (const auto& sm : filtered_score_members) {
      ZSetsDataKey zsets_data_key(key, version, sm.member);
      const void* ptr_score = reinterpret_cast<const void*>(&sm.score);
      EncodeFixed64(score_buf, *reinterpret_cast<const uint64_t*>(ptr_score));
      batch.Put(handles_[1], zsets_data_key.Encode(), Slice(score_buf, sizeof(uint64_t)));

      ZSetsScoreKey zsets_score_key(key, version, sm.score, sm.member);
      batch.Put(handles_[2], zsets_score_key.Encode(), Slice());
    }
    *ret = filtered_score_members.size();
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisZSets::ZScore(const Slice& key, const Slice& member, double* score) {

  *score = 0;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s = db_->Get(read_options, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    int32_t version = parsed_zsets_meta_value.version();
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      std::string data_value;
      ZSetsDataKey zsets_data_key(key, version, member);
      s = db_->Get(read_options, handles_[1], zsets_data_key.Encode(), &data_value);
      if (s.ok()) {
        uint64_t tmp = DecodeFixed64(data_value.data());
        const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
        *score = *reinterpret_cast<const double*>(ptr_tmp);
      } else {
        return s;
      }
    }
  } else if (!s.IsNotFound()) {
    return s;
  }
  return s;
}

Status RedisZSets::ZCard(const Slice& key, int32_t* card) {
  *card = 0;
  std::string meta_value;

  Status s = db_->Get(default_read_options_, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      *card = 0;
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      *card = 0;
      return Status::NotFound();
    } else {
      *card = parsed_zsets_meta_value.count();
    }
  }
  return s;
}

Status RedisZSets::ZRange(const Slice& key,
                          int32_t start,
                          int32_t stop,
                          std::vector<BlackWidow::ScoreMember>* score_members) {
  score_members->clear();
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot = nullptr;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  Status s = db_->Get(read_options, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_zsets_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t count = parsed_zsets_meta_value.count();
      int32_t version = parsed_zsets_meta_value.version();
      int32_t start_index = start >= 0 ? start : count + start;
      int32_t stop_index  = stop  >= 0 ? stop  : count + stop;
      start_index = start_index <= 0 ? 0 : start_index;
      stop_index = stop_index >= count ? count - 1 : stop_index;
      if (start_index > stop_index
        || start_index >= count
        || stop_index < 0) {
        return s;
      }
      int32_t cur_index = 0;
      BlackWidow::ScoreMember score_member;
      ZSetsScoreKey zsets_score_key(key, version, std::numeric_limits<double>::lowest(), Slice());
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(zsets_score_key.Encode());
           iter->Valid() && cur_index <= stop_index;
           iter->Next(), ++cur_index) {
        if (cur_index >= start_index) {
          ParsedZSetsScoreKey parsed_zsets_score_key(iter->key());
          score_member.score = parsed_zsets_score_key.score();
          score_member.member = parsed_zsets_score_key.member().ToString();
          score_members->push_back(score_member);
        }
      }
      delete iter;
    }
  }
  return s;
}

Status RedisZSets::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound();
    }
    if (ttl > 0) {
      parsed_zsets_meta_value.SetRelativeTimestamp(ttl);
    } else {
      parsed_zsets_meta_value.InitialMetaValue();
    }
    s = db_->Put(default_write_options_, handles_[0], key, meta_value);
  }
  return s;
}

Status RedisZSets::Del(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &meta_value);
  if (s.ok()) {
    ParsedZSetsMetaValue parsed_zsets_meta_value(&meta_value);
    if (parsed_zsets_meta_value.IsStale()) {
      return Status::NotFound();
    } else {
      parsed_zsets_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

bool RedisZSets::Scan(const std::string& start_key,
                        const std::string& pattern,
                        std::vector<std::string>* keys,
                        int64_t* count,
                        std::string* next_key) {
  //TODO: to be implemented
  return true;
}

Status RedisZSets::Expireat(const Slice& key, int32_t timestamp) {
  //TODO: to be implemented
  return Status();
}
Status RedisZSets::Persist(const Slice& key) {
  //TODO: to be implemented
  return Status();
}
Status RedisZSets::TTL(const Slice& key, int64_t* timestamp) {
  //TODO: to be implemented
  return Status();
}

} // blackwidow

