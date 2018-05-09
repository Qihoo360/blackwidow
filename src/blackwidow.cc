//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "blackwidow/blackwidow.h"

#include "src/mutex_impl.h"
#include "src/redis_strings.h"
#include "src/redis_hashes.h"
#include "src/redis_sets.h"
#include "src/redis_lists.h"
#include "src/redis_zsets.h"
#include "src/redis_hyperloglog.h"

namespace blackwidow {

BlackWidow::BlackWidow() :
  strings_db_(nullptr),
  hashes_db_(nullptr),
  sets_db_(nullptr),
  mutex_factory_(new MutexFactoryImpl) {
  cursors_store_.max_size_ = 5000;
  cursors_mutex_ = mutex_factory_->AllocateMutex();
}

BlackWidow::~BlackWidow() {
  delete strings_db_;
  delete hashes_db_;
  delete sets_db_;
  delete lists_db_;
  delete zsets_db_;
}

Status BlackWidow::Compact() {
  Status s = strings_db_->CompactRange(NULL, NULL);
  if (!s.ok()) {
    return s;
  }
  return hashes_db_->CompactRange(NULL, NULL);
}

static std::string AppendSubDirectory(const std::string& db_path,
    const std::string& sub_db) {
  if (db_path.back() == '/') {
    return db_path + sub_db;
  } else {
    return db_path + "/" + sub_db;
  }
}

Status BlackWidow::Open(const rocksdb::Options& options,
    const std::string& db_path) {
  strings_db_ = new RedisStrings();
  Status s = strings_db_->Open(options, AppendSubDirectory(db_path, "strings"));
  hashes_db_ = new RedisHashes();
  s = hashes_db_->Open(options, AppendSubDirectory(db_path, "hashes"));
  sets_db_ = new RedisSets();
  s = sets_db_->Open(options, AppendSubDirectory(db_path, "sets"));
  lists_db_ = new RedisLists();
  s = lists_db_->Open(options, AppendSubDirectory(db_path, "lists"));
  zsets_db_ = new RedisZSets();
  s = zsets_db_->Open(options, AppendSubDirectory(db_path, "zsets"));
  return s;
}

Status BlackWidow::GetStartKey(int64_t cursor, std::string* start_key) {
  cursors_mutex_->Lock();
  if (cursors_store_.map_.end() == cursors_store_.map_.find(cursor)) {
    cursors_mutex_->UnLock();
    return Status::NotFound();
  } else {
    // If the cursor is present in the list,
    // move the cursor to the start of list
    cursors_store_.list_.remove(cursor);
    cursors_store_.list_.push_front(cursor);
    *start_key = cursors_store_.map_[cursor];
    cursors_mutex_->UnLock();
    return Status::OK();
  }
}

int64_t BlackWidow::StoreAndGetCursor(int64_t cursor,
                                      const std::string& next_key) {
  cursors_mutex_->Lock();
  if (cursors_store_.map_.size() >
      static_cast<size_t>(cursors_store_.max_size_)) {
    int64_t tail = cursors_store_.list_.back();
    cursors_store_.list_.remove(tail);
    cursors_store_.map_.erase(tail);
  }

  cursors_store_.list_.push_back(cursor);
  cursors_store_.map_[cursor] = next_key;
  cursors_mutex_->UnLock();
  return cursor;
}

// Strings Commands
Status BlackWidow::Set(const Slice& key, const Slice& value) {
  return strings_db_->Set(key, value);
}

Status BlackWidow::Get(const Slice& key, std::string* value) {
  return strings_db_->Get(key, value);
}

Status BlackWidow::GetSet(const Slice& key, const Slice& value,
                          std::string* old_value) {
  return strings_db_->GetSet(key, value, old_value);
}

Status BlackWidow::SetBit(const Slice& key, int64_t offset,
                          int32_t value, int32_t* ret) {
  return strings_db_->SetBit(key, offset, value, ret);
}

Status BlackWidow::GetBit(const Slice& key, int64_t offset, int32_t* ret) {
  return strings_db_->GetBit(key, offset, ret);
}

Status BlackWidow::MSet(const std::vector<BlackWidow::KeyValue>& kvs) {
  return strings_db_->MSet(kvs);
}

Status BlackWidow::MGet(const std::vector<std::string>& keys,
                        std::vector<std::string>* values) {
  return strings_db_->MGet(keys, values);
}

Status BlackWidow::Setnx(const Slice& key, const Slice& value, int32_t* ret) {
  return strings_db_->Setnx(key, value, ret);
}

Status BlackWidow::MSetnx(const std::vector<BlackWidow::KeyValue>& kvs,
                          int32_t* ret) {
  return strings_db_->MSetnx(kvs, ret);
}

Status BlackWidow::Setrange(const Slice& key, int64_t start_offset,
                            const Slice& value, int32_t* ret) {
  return strings_db_->Setrange(key, start_offset, value, ret);
}

Status BlackWidow::Getrange(const Slice& key, int64_t start_offset,
                            int64_t end_offset, std::string* ret) {
  return strings_db_->Getrange(key, start_offset, end_offset, ret);
}

Status BlackWidow::Append(const Slice& key, const Slice& value, int32_t* ret) {
  return strings_db_->Append(key, value, ret);
}

Status BlackWidow::BitCount(const Slice& key, int64_t start_offset,
                            int64_t end_offset, int32_t *ret, bool have_range) {
  return strings_db_->BitCount(key, start_offset, end_offset, ret, have_range);
}

Status BlackWidow::BitOp(BitOpType op, const std::string& dest_key,
                         const std::vector<std::string>& src_keys,
                         int64_t* ret) {
  return strings_db_->BitOp(op, dest_key, src_keys, ret);
}

Status BlackWidow::BitPos(const Slice& key, int32_t bit,
                          int64_t* ret) {
  return strings_db_->BitPos(key, bit, ret);
}

Status BlackWidow::BitPos(const Slice& key, int32_t bit,
                          int64_t start_offset, int64_t* ret) {
  return strings_db_->BitPos(key, bit, start_offset, ret);
}

Status BlackWidow::BitPos(const Slice& key, int32_t bit,
                          int64_t start_offset, int64_t end_offset,
                          int64_t* ret) {
  return strings_db_->BitPos(key, bit, start_offset, end_offset, ret);
}

Status BlackWidow::Decrby(const Slice& key, int64_t value, int64_t* ret) {
  return strings_db_->Decrby(key, value, ret);
}

Status BlackWidow::Incrby(const Slice& key, int64_t value, int64_t* ret) {
  return strings_db_->Incrby(key, value, ret);
}

Status BlackWidow::Incrbyfloat(const Slice& key, const Slice& value,
                               std::string* ret) {
  return strings_db_->Incrbyfloat(key, value, ret);
}

Status BlackWidow::Setex(const Slice& key, const Slice& value, int32_t ttl) {
  return strings_db_->Setex(key, value, ttl);
}

Status BlackWidow::Strlen(const Slice& key, int32_t* len) {
  return strings_db_->Strlen(key, len);
}

// Hashes Commands
Status BlackWidow::HSet(const Slice& key, const Slice& field,
    const Slice& value, int32_t* res) {
  return hashes_db_->HSet(key, field, value, res);
}

Status BlackWidow::HGet(const Slice& key, const Slice& field,
    std::string* value) {
  return hashes_db_->HGet(key, field, value);
}

Status BlackWidow::HMSet(const Slice& key,
                         const std::vector<BlackWidow::FieldValue>& fvs) {
  return hashes_db_->HMSet(key, fvs);
}

Status BlackWidow::HMGet(const Slice& key,
                         const std::vector<std::string>& fields,
                         std::vector<std::string>* values) {
  return hashes_db_->HMGet(key, fields, values);
}

Status BlackWidow::HGetall(const Slice& key,
                           std::vector<BlackWidow::FieldValue>* fvs) {
  return hashes_db_->HGetall(key, fvs);
}

Status BlackWidow::HKeys(const Slice& key,
                         std::vector<std::string>* fields) {
  return hashes_db_->HKeys(key, fields);
}

Status BlackWidow::HVals(const Slice& key,
                         std::vector<std::string>* values) {
  return hashes_db_->HVals(key, values);
}

Status BlackWidow::HSetnx(const Slice& key, const Slice& field,
                          const Slice& value, int32_t* ret) {
  return hashes_db_->HSetnx(key, field, value, ret);
}

Status BlackWidow::HLen(const Slice& key, int32_t* ret) {
  return hashes_db_->HLen(key, ret);
}

Status BlackWidow::HStrlen(const Slice& key, const Slice& field, int32_t* len) {
  return hashes_db_->HStrlen(key, field, len);
}

Status BlackWidow::HExists(const Slice& key, const Slice& field) {
  return hashes_db_->HExists(key, field);
}

Status BlackWidow::HIncrby(const Slice& key, const Slice& field, int64_t value,
                           int64_t* ret) {
  return hashes_db_->HIncrby(key, field, value, ret);
}

Status BlackWidow::HIncrbyfloat(const Slice& key, const Slice& field,
                                const Slice& by, std::string* new_value) {
  return hashes_db_->HIncrbyfloat(key, field, by, new_value);
}

Status BlackWidow::HDel(const Slice& key,
                        const std::vector<std::string>& fields,
                        int32_t* ret) {
  return hashes_db_->HDel(key, fields, ret);
}

// Sets Commands
Status BlackWidow::SAdd(const Slice& key,
                        const std::vector<std::string>& members,
                        int32_t* ret) {
  return sets_db_->SAdd(key, members, ret);
}

Status BlackWidow::SCard(const Slice& key,
                         int32_t* ret) {
  return sets_db_->SCard(key, ret);
}

Status BlackWidow::SDiff(const std::vector<std::string>& keys,
                         std::vector<std::string>* members) {
  return sets_db_->SDiff(keys, members);
}

Status BlackWidow::SDiffstore(const Slice& destination,
                              const std::vector<std::string>& keys,
                              int32_t* ret) {
  return sets_db_->SDiffstore(destination, keys, ret);
}

Status BlackWidow::SInter(const std::vector<std::string>& keys,
                          std::vector<std::string>* members) {
  return sets_db_->SInter(keys, members);
}

Status BlackWidow::SInterstore(const Slice& destination,
                               const std::vector<std::string>& keys,
                               int32_t* ret) {
  return sets_db_->SInterstore(destination, keys, ret);
}

Status BlackWidow::SIsmember(const Slice& key, const Slice& member,
                             int32_t* ret) {
  return sets_db_->SIsmember(key, member, ret);
}

Status BlackWidow::SMembers(const Slice& key,
                            std::vector<std::string>* members) {
  return sets_db_->SMembers(key, members);
}

Status BlackWidow::SMove(const Slice& source, const Slice& destination,
                         const Slice& member, int32_t* ret) {
  return sets_db_->SMove(source, destination, member, ret);
}

Status BlackWidow::SPop(const Slice& key, int32_t count,
                        std::vector<std::string>* members) {
  return sets_db_->SPop(key, count, members);
}

Status BlackWidow::SRandmembers(const Slice& key, int32_t count,
                                std::vector<std::string>* members) {
  return sets_db_->SRandmembers(key, count, members);
}

Status BlackWidow::SRem(const Slice& key,
                        const std::vector<std::string>& members,
                        int32_t* ret) {
  return sets_db_->SRem(key, members, ret);
}

Status BlackWidow::SUnion(const std::vector<std::string>& keys,
                          std::vector<std::string>* members) {
  return sets_db_->SUnion(keys, members);
}

Status BlackWidow::SUnionstore(const Slice& destination,
                               const std::vector<std::string>& keys,
                               int32_t* ret) {
  return sets_db_->SUnionstore(destination, keys, ret);
}

Status BlackWidow::LPush(const Slice& key,
                         const std::vector<std::string>& values,
                         uint64_t* ret) {
  return lists_db_->LPush(key, values, ret);
}

Status BlackWidow::RPush(const Slice& key,
                         const std::vector<std::string>& values,
                         uint64_t* ret) {
  return lists_db_->RPush(key, values, ret);
}

Status BlackWidow::LRange(const Slice& key, int64_t start, int64_t stop,
                          std::vector<std::string>* ret) {
  return lists_db_->LRange(key, start, stop, ret);
}

Status BlackWidow::LTrim(const Slice& key, int64_t start, int64_t stop) {
  return lists_db_->LTrim(key, start, stop);
}

Status BlackWidow::LLen(const Slice& key, uint64_t* len) {
  return lists_db_->LLen(key, len);
}

Status BlackWidow::LPop(const Slice& key, std::string* element) {
  return lists_db_->LPop(key, element);
}

Status BlackWidow::RPop(const Slice& key, std::string* element) {
  return lists_db_->RPop(key, element);
}

Status BlackWidow::LIndex(const Slice& key,
                          int64_t index,
                          std::string* element) {
  return lists_db_->LIndex(key, index, element);
}

Status BlackWidow::LInsert(const Slice& key,
                           const BeforeOrAfter& before_or_after,
                           const std::string& pivot,
                           const std::string& value,
                           int64_t* ret) {
  return lists_db_->LInsert(key, before_or_after, pivot, value, ret);
}

Status BlackWidow::LPushx(const Slice& key, const Slice& value, uint64_t* len) {
  return lists_db_->LPushx(key, value, len);
}

Status BlackWidow::RPushx(const Slice& key, const Slice& value, uint64_t* len) {
  return lists_db_->RPushx(key, value, len);
}

Status BlackWidow::LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* ret) {
  return lists_db_->LRem(key, count, value, ret);
}

Status BlackWidow::LSet(const Slice& key, int64_t index, const Slice& value) {
  return lists_db_->LSet(key, index, value);
}

Status BlackWidow::RPoplpush(const Slice& source,
                             const Slice& destination,
                             std::string* element) {
  return lists_db_->RPoplpush(source, destination, element);
}

Status BlackWidow::ZAdd(const Slice& key,
                        const std::vector<ScoreMember>& score_members,
                        int32_t* ret) {
  return zsets_db_->ZAdd(key, score_members, ret);
}

Status BlackWidow::ZScore(const Slice& key,
                          const Slice& member,
                          double* ret) {
  return zsets_db_->ZScore(key, member, ret);
}

Status BlackWidow::ZCard(const Slice& key,
                         int32_t* ret) {
  return zsets_db_->ZCard(key, ret);
}

Status BlackWidow::ZRange(const Slice& key,
                          int32_t start,
                          int32_t stop,
                          std::vector<ScoreMember>* score_members) {
  return zsets_db_->ZRange(key, start, stop, score_members);
}


// Keys Commands
int32_t BlackWidow::Expire(const Slice& key, int32_t ttl,
                           std::map<DataType, Status>* type_status) {
  int32_t ret = 0;
  bool is_corruption = false;

  // Strings
  Status s = strings_db_->Expire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kStrings] = s;
  }

  // Hash
  s = hashes_db_->Expire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kHashes] = s;
  }

  // Sets
  s = sets_db_->Expire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kSets] = s;
  }

  // Lists
  s = lists_db_->Expire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if(!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kLists] = s;
  }

  // Zsets
  s = zsets_db_->Expire(key, ttl);
  if (s.ok()) {
    ret++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kZSets] = s;
  }

  if (is_corruption) {
    return -1;
  } else {
    return ret;
  }
}

int64_t BlackWidow::Del(const std::vector<std::string>& keys,
                        std::map<DataType, Status>* type_status) {
  Status s;
  int64_t count = 0;
  bool is_corruption = false;

  for (const auto& key : keys) {
    // Strings
    Status s = strings_db_->Del(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kStrings] = s;
    }

    // Hashes
    s = hashes_db_->Del(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kHashes] = s;
    }

    // Sets
    s = sets_db_->Del(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kSets] = s;
    }

    // Lists
    s = lists_db_->Del(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kLists] = s;
    }

    // ZSets
    s = zsets_db_->Del(key);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kZSets] = s;
    }
  }

  if (is_corruption) {
    return -1;
  } else {
    return count;
  }
}

int64_t BlackWidow::Exists(const std::vector<std::string>& keys,
                       std::map<DataType, Status>* type_status) {
  int64_t count = 0;
  int32_t ret;
  uint64_t llen;
  std::string value;
  Status s;
  bool is_corruption = false;

  for (const auto& key : keys) {
    s = strings_db_->Get(key, &value);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kStrings] = s;
    }

    s = hashes_db_->HLen(key, &ret);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kHashes] = s;
    }

    s = sets_db_->SCard(key, &ret);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kSets] = s;
    }

    s = lists_db_->LLen(key, &llen);
    if (s.ok()) {
      count++;
    } else if (!s.IsNotFound()) {
      is_corruption = true;
      (*type_status)[DataType::kLists] = s;
    }

    // TODO(wxj) other types
  }

  if (is_corruption) {
    return -1;
  } else {
    return count;
  }
}

int64_t BlackWidow::Scan(int64_t cursor, const std::string& pattern,
                         int64_t count, std::vector<std::string>* keys) {
  bool is_finish;
  int64_t count_origin = count, cursor_ret = 0;
  std::string start_key = "";
  std::string next_key;

  if (cursor < 0) {
    return cursor_ret;
  } else {
    Status s = GetStartKey(cursor, &start_key);
    if (s.IsNotFound()) {
      start_key = "k";
      cursor = 0;
    }
  }

  char key_type = start_key.at(0);
  start_key.erase(start_key.begin());
  switch (key_type) {
    case 'k':
      is_finish = strings_db_->Scan(start_key, pattern, keys,
                                    &count, &next_key);
      if (count == 0 && is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + count_origin, std::string("h"));
        break;
      } else if (count == 0 && !is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + count_origin,
                                       std::string("k") + next_key);
        break;
      }
      start_key = "";
    case 'h':
      is_finish = hashes_db_->Scan(start_key, pattern, keys,
                                   &count, &next_key);
      if (count == 0 && is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + count_origin, std::string("s"));
        break;
      } else if (count == 0 && !is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + count_origin,
                                       std::string("h") + next_key);
        break;
      }
      start_key = "";
    case 's':
      is_finish = sets_db_->Scan(start_key, pattern, keys,
                                   &count, &next_key);
      if (count == 0 && is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + count_origin, std::string("l"));
        break;
      } else if (count == 0 && !is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + count_origin,
                                       std::string("s") + next_key);
        break;
      }
      start_key = "";
    case 'l':
      is_finish = lists_db_->Scan(start_key, pattern, keys,
                                    &count, &next_key);
      if (count == 0 && is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + count_origin, std::string("z"));
        break;
      } else if (count == 0 && !is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + count_origin,
                                       std::string("l") + next_key);
        break;
      }
      start_key = "";
    // TODO(wxj) other data types
  }

  return cursor_ret;
}

int32_t BlackWidow::Expireat(const Slice& key, int32_t timestamp,
                             std::map<DataType, Status>* type_status) {
  Status s;
  int32_t count = 0;
  bool is_corruption = false;

  s = strings_db_->Expireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kStrings] = s;
  }

  s = hashes_db_->Expireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kHashes] = s;
  }

  s = sets_db_->Expireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kSets] = s;
  }

  // TODO(shq) other types
  if (is_corruption) {
    return -1;
  } else {
    return count;
  }
}

int32_t BlackWidow::Persist(const Slice& key,
                            std::map<DataType, Status>* type_status) {
  Status s;
  int32_t count = 0;
  bool is_corruption = false;

  s = strings_db_->Persist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kStrings] = s;
  }

  s = hashes_db_->Persist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kHashes] = s;
  }

  s = sets_db_->Persist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kSets] = s;
  }

  s = lists_db_->Persist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kLists] = s;
  }


  // TODO(wxj) other types
  if (is_corruption) {
    return -1;
  } else {
    return count;
  }
}

std::map<BlackWidow::DataType, int64_t> BlackWidow::TTL(const Slice& key,
                        std::map<BlackWidow::DataType, Status>* type_status) {
  Status s;
  std::map<DataType, int64_t> ret;
  int64_t timestamp = 0;

  s = strings_db_->TTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kStrings] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kStrings] = -3;
    (*type_status)[DataType::kStrings] = s;
  }

  s = hashes_db_->TTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kHashes] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kHashes] = -3;
    (*type_status)[DataType::kHashes] = s;
  }

  s = sets_db_->TTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kSets] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kSets] = -3;
    (*type_status)[DataType::kSets] = s;
  }

  s = lists_db_->TTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kLists] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kLists] = -3;
    (*type_status)[DataType::kLists] = s;
  }

  // TODO(wxj) other types
  return ret;
}

// HyperLogLog
Status BlackWidow::PfAdd(const Slice& key,
                         const std::vector<std::string>& values, bool* update) {
  *update = false;
  if (values.size() >= kMaxKeys) {
    return Status::InvalidArgument("Invalid the number of key");
  }

  std::string value, registers, result = "";
  Status s = strings_db_->Get(key, &value);
  if (s.ok()) {
    registers = value;
  } else if (s.IsNotFound()) {
    registers = "";
  } else {
    return s;
  }
  HyperLogLog log(kPrecision, registers);
  int32_t previous = static_cast<int32_t>(log.Estimate());
  for (size_t i = 0; i < values.size(); ++i) {
    result = log.Add(values[i].data(), values[i].size());
  }
  HyperLogLog update_log(kPrecision, result);
  int32_t now = static_cast<int32_t>(update_log.Estimate());
  if (previous != now || (s.IsNotFound() && values.size() == 0)) {
    *update = true;
  }
  s = strings_db_->Set(key, result);
  return s;
}

Status BlackWidow::PfCount(const std::vector<std::string>& keys,
                           int64_t* result) {
  if (keys.size() >= kMaxKeys || keys.size() <= 0) {
    return Status::InvalidArgument("Invalid the number of key");
  }

  std::string value, first_registers;
  Status s = strings_db_->Get(keys[0], &value);
  if (s.ok()) {
    first_registers = std::string(value.data(), value.size());
  } else if (s.IsNotFound()) {
    first_registers = "";
  }

  HyperLogLog first_log(kPrecision, first_registers);
  for (size_t i = 1; i < keys.size(); ++i) {
    std::string value, registers;
    s = strings_db_->Get(keys[i], &value);
    if (s.ok()) {
      registers = value;
    } else if (s.IsNotFound()) {
      continue;
    } else {
      return s;
    }
    HyperLogLog log(kPrecision, registers);
    first_log.Merge(log);
  }
  *result = static_cast<int32_t>(first_log.Estimate());
  return Status::OK();
}

Status BlackWidow::PfMerge(const std::vector<std::string>& keys) {
  if (keys.size() >= kMaxKeys || keys.size() <= 0) {
    return Status::InvalidArgument("Invalid the number of key");
  }

  Status s;
  std::string value, first_registers, result;
  s = strings_db_->Get(keys[0], &value);
  if (s.ok()) {
    first_registers = std::string(value.data(), value.size());
  } else if (s.IsNotFound()) {
    first_registers = "";
  }

  result = first_registers;
  HyperLogLog first_log(kPrecision, first_registers);
  for (size_t i = 1; i < keys.size(); ++i) {
    std::string value, registers;
    s = strings_db_->Get(keys[i], &value);
    if (s.ok()) {
      registers = std::string(value.data(), value.size());
    } else if (s.IsNotFound()) {
      continue;
    } else {
      return s;
    }
    HyperLogLog log(kPrecision, registers);
    result = first_log.Merge(log);
  }
  s = strings_db_->Set(keys[0], result);
  return s;
}

}  //  namespace blackwidow
