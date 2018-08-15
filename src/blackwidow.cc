//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "blackwidow/blackwidow.h"
#include "blackwidow/util.h"

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
  zsets_db_(nullptr),
  lists_db_(nullptr),
  mutex_factory_(new MutexFactoryImpl),
  bg_tasks_cond_var_(&bg_tasks_mutex_),
  current_task_type_(0),
  bg_tasks_should_exit_(false),
  scan_keynum_exit_(false) {

  cursors_store_.max_size_ = 5000;
  cursors_mutex_ = mutex_factory_->AllocateMutex();

  Status s = StartBGThread();
  if (!s.ok()) {
    fprintf (stderr, "[FATAL] start bg thread failed, %s\n", s.ToString().c_str());
    exit(-1);
  }
}

BlackWidow::~BlackWidow() {

  bg_tasks_should_exit_ = true;
  bg_tasks_cond_var_.Signal();

  int ret = 0;
  if ((ret = pthread_join(bg_tasks_thread_id_, NULL)) != 0) {
    fprintf (stderr, "pthread_join failed with bgtask thread error %d\n", ret);
  }

  delete strings_db_;
  delete hashes_db_;
  delete sets_db_;
  delete lists_db_;
  delete zsets_db_;
  delete mutex_factory_;
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
  mkpath(db_path.c_str(), 0755);

  strings_db_ = new RedisStrings();
  Status s = strings_db_->Open(options, AppendSubDirectory(db_path, "strings"));
  if (!s.ok()) {
    fprintf (stderr, "[FATAL] open kv db failed, %s\n", s.ToString().c_str());
    exit(-1);
  }

  hashes_db_ = new RedisHashes();
  s = hashes_db_->Open(options, AppendSubDirectory(db_path, "hashes"));
  if (!s.ok()) {
    fprintf (stderr, "[FATAL] open hashes db failed, %s\n", s.ToString().c_str());
    exit(-1);
  }

  sets_db_ = new RedisSets();
  s = sets_db_->Open(options, AppendSubDirectory(db_path, "sets"));
  if (!s.ok()) {
    fprintf (stderr, "[FATAL] open set db failed, %s\n", s.ToString().c_str());
    exit(-1);
  }

  lists_db_ = new RedisLists();
  s = lists_db_->Open(options, AppendSubDirectory(db_path, "lists"));
  if (!s.ok()) {
    fprintf (stderr, "[FATAL] open list db failed, %s\n", s.ToString().c_str());
    exit(-1);
  }

  zsets_db_ = new RedisZSets();
  s = zsets_db_->Open(options, AppendSubDirectory(db_path, "zsets"));
  if (!s.ok()) {
    fprintf (stderr, "[FATAL] open zset db failed, %s\n", s.ToString().c_str());
    exit(-1);
  }
  return Status::OK();
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
  if (cursors_store_.map_.size() > cursors_store_.max_size_) {
    int64_t tail = cursors_store_.list_.back();
    cursors_store_.list_.remove(tail);
    cursors_store_.map_.erase(tail);
  }

  cursors_store_.list_.push_front(cursor);
  cursors_store_.map_[cursor] = next_key;
  cursors_mutex_->UnLock();
  return cursor;
}

// Strings Commands
Status BlackWidow::Set(const Slice& key, const Slice& value, const int32_t ttl) {
  return strings_db_->Set(key, value, ttl);
}

Status BlackWidow::Setxx(const Slice& key, const Slice& value, int32_t* ret, const int32_t ttl) {
  return strings_db_->Setxx(key, value, ret, ttl);
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

Status BlackWidow::MSet(const std::vector<KeyValue>& kvs) {
  return strings_db_->MSet(kvs);
}

Status BlackWidow::MGet(const std::vector<std::string>& keys,
                        std::vector<std::string>* values) {
  return strings_db_->MGet(keys, values);
}

Status BlackWidow::Setnx(const Slice& key, const Slice& value, int32_t* ret, const int32_t ttl) {
  return strings_db_->Setnx(key, value, ret, ttl);
}

Status BlackWidow::MSetnx(const std::vector<KeyValue>& kvs,
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
                         const std::vector<FieldValue>& fvs) {
  return hashes_db_->HMSet(key, fvs);
}

Status BlackWidow::HMGet(const Slice& key,
                         const std::vector<std::string>& fields,
                         std::vector<std::string>* values) {
  return hashes_db_->HMGet(key, fields, values);
}

Status BlackWidow::HGetall(const Slice& key,
                           std::vector<FieldValue>* fvs) {
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

// See SCAN for HSCAN documentation.
Status BlackWidow::HScan(const Slice& key, int64_t cursor, const std::string& pattern,
                         int64_t count, std::vector<FieldValue>* field_values, int64_t* next_cursor) {
  return hashes_db_->HScan(key, cursor, pattern, count, field_values, next_cursor);
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

Status BlackWidow::SPop(const Slice& key, std::string* member) {
  bool need_compact = false;
  Status status = sets_db_->SPop(key, member, &need_compact);
  if (need_compact) {
    AddBGTask({kSets, kCompactKey, key.ToString()});
  }
  return status;
}

Status BlackWidow::SRandmember(const Slice& key, int32_t count,
                               std::vector<std::string>* members) {
  return sets_db_->SRandmember(key, count, members);
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

Status BlackWidow::SScan(const Slice& key, int64_t cursor, const std::string& pattern,
                         int64_t count, std::vector<std::string>* members, int64_t* next_cursor) {
  return sets_db_->SScan(key, cursor, pattern, count, members, next_cursor);
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

Status BlackWidow::ZCard(const Slice& key,
                         int32_t* ret) {
  return zsets_db_->ZCard(key, ret);
}

Status BlackWidow::ZCount(const Slice& key,
                          double min,
                          double max,
                          bool left_close,
                          bool right_close,
                          int32_t* ret) {
  return zsets_db_->ZCount(key, min, max, left_close, right_close, ret);
}

Status BlackWidow::ZIncrby(const Slice& key,
                           const Slice& member,
                           double increment,
                           double* ret) {
  return zsets_db_->ZIncrby(key, member, increment, ret);
}

Status BlackWidow::ZRange(const Slice& key,
                          int32_t start,
                          int32_t stop,
                          std::vector<ScoreMember>* score_members) {
  return zsets_db_->ZRange(key, start, stop, score_members);
}

Status BlackWidow::ZRangebyscore(const Slice& key,
                                 double min,
                                 double max,
                                 bool left_close,
                                 bool right_close,
                                 std::vector<ScoreMember>* score_members) {
  return zsets_db_->ZRangebyscore(key, min, max, left_close, right_close, score_members);
}

Status BlackWidow::ZRank(const Slice& key,
                         const Slice& member,
                         int32_t* rank) {
  return zsets_db_->ZRank(key, member, rank);
}

Status BlackWidow::ZRem(const Slice& key,
                        std::vector<std::string> members,
                        int32_t* ret) {
  return zsets_db_->ZRem(key, members, ret);
}

Status BlackWidow::ZRemrangebyrank(const Slice& key,
                                   int32_t start,
                                   int32_t stop,
                                   int32_t* ret) {
  return zsets_db_->ZRemrangebyrank(key, start, stop, ret);
}

Status BlackWidow::ZRemrangebyscore(const Slice& key,
                                    double min,
                                    double max,
                                    bool left_close,
                                    bool right_close,
                                    int32_t* ret) {
  return zsets_db_->ZRemrangebyscore(key, min, max, left_close, right_close, ret);
}

Status BlackWidow::ZRevrange(const Slice& key,
                             int32_t start,
                             int32_t stop,
                             std::vector<ScoreMember>* score_members) {
  return zsets_db_->ZRevrange(key, start, stop, score_members);
}

Status BlackWidow::ZRevrangebyscore(const Slice& key,
                                    double min,
                                    double max,
                                    bool left_close,
                                    bool right_close,
                                    std::vector<ScoreMember>* score_members) {
  return zsets_db_->ZRevrangebyscore(key, min, max, left_close, right_close, score_members);
}

Status BlackWidow::ZRevrank(const Slice& key,
                            const Slice& member,
                            int32_t* rank) {
  return zsets_db_->ZRevrank(key, member, rank);
}

Status BlackWidow::ZScore(const Slice& key,
                          const Slice& member,
                          double* ret) {
  return zsets_db_->ZScore(key, member, ret);
}

Status BlackWidow::ZUnionstore(const Slice& destination,
                               const std::vector<std::string>& keys,
                               const std::vector<double>& weights,
                               const AGGREGATE agg,
                               int32_t* ret) {
  return zsets_db_->ZUnionstore(destination, keys, weights, agg, ret);
}

Status BlackWidow::ZInterstore(const Slice& destination,
                               const std::vector<std::string>& keys,
                               const std::vector<double>& weights,
                               const AGGREGATE agg,
                               int32_t* ret) {
  return zsets_db_->ZInterstore(destination, keys, weights, agg, ret);
}

Status BlackWidow::ZRangebylex(const Slice& key,
                               const Slice& min,
                               const Slice& max,
                               bool left_close,
                               bool right_close,
                               std::vector<std::string>* members) {
  return zsets_db_->ZRangebylex(key, min, max, left_close, right_close, members);
}

Status BlackWidow::ZLexcount(const Slice& key,
                             const Slice& min,
                             const Slice& max,
                             bool left_close,
                             bool right_close,
                             int32_t* ret) {
  return zsets_db_->ZLexcount(key, min, max, left_close, right_close, ret);
}

Status BlackWidow::ZRemrangebylex(const Slice& key,
                                  const Slice& min,
                                  const Slice& max,
                                  bool left_close,
                                  bool right_close,
                                  int32_t* ret) {
  return zsets_db_->ZRemrangebylex(key, min, max, left_close, right_close, ret);
}

Status BlackWidow::ZScan(const Slice& key, int64_t cursor, const std::string& pattern,
                         int64_t count, std::vector<ScoreMember>* score_members, int64_t* next_cursor) {
  return zsets_db_->ZScan(key, cursor, pattern, count, score_members, next_cursor);
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

int64_t BlackWidow::DelByType(const std::vector<std::string>& keys,
                              DataType type) {
  Status s;
  int64_t count = 0;
  bool is_corruption = false;

  for (const auto& key : keys) {
    switch (type) {
      // Strings
      case DataType::kStrings:
      {
        s = strings_db_->Del(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      // Hashes
      case DataType::kHashes:
      {
        s = hashes_db_->Del(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      // Sets
      case DataType::kSets:
      {
        s = sets_db_->Del(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      // Lists
      case DataType::kLists:
      {
        s = lists_db_->Del(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      // ZSets
      case DataType::kZSets:
      {
        s = zsets_db_->Del(key);
        if (s.ok()) {
          count++;
        } else if (!s.IsNotFound()) {
          is_corruption = true;
        }
        break;
      }
      case DataType::kAll:
      {
        return -1;
      }
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

    s = zsets_db_->ZCard(key, &ret);
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

int64_t BlackWidow::Scan(int64_t cursor, const std::string& pattern,
                         int64_t count, std::vector<std::string>* keys) {
  bool is_finish;
  int64_t step_length = count, cursor_ret = 0;
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
        cursor_ret = StoreAndGetCursor(cursor + step_length, std::string("h"));
        break;
      } else if (count == 0 && !is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + step_length, std::string("k") + next_key);
        break;
      }
      start_key = "";
    case 'h':
      is_finish = hashes_db_->Scan(start_key, pattern, keys,
                                   &count, &next_key);
      if (count == 0 && is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + step_length, std::string("s"));
        break;
      } else if (count == 0 && !is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + step_length, std::string("h") + next_key);
        break;
      }
      start_key = "";
    case 's':
      is_finish = sets_db_->Scan(start_key, pattern, keys,
                                   &count, &next_key);
      if (count == 0 && is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + step_length, std::string("l"));
        break;
      } else if (count == 0 && !is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + step_length, std::string("s") + next_key);
        break;
      }
      start_key = "";
    case 'l':
      is_finish = lists_db_->Scan(start_key, pattern, keys,
                                    &count, &next_key);
      if (count == 0 && is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + step_length, std::string("z"));
        break;
      } else if (count == 0 && !is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + step_length, std::string("l") + next_key);
        break;
      }
      start_key = "";
    case 'z':
      is_finish = zsets_db_->Scan(start_key, pattern, keys,
                                  &count, &next_key);
      if (is_finish) {
        cursor_ret = 0;
        break;
      } else if (count == 0 && !is_finish) {
        cursor_ret = StoreAndGetCursor(cursor + step_length, std::string("z") + next_key);
        break;
      }
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

  s = lists_db_->Expireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kLists] = s;
  }

  s = zsets_db_->Expireat(key, timestamp);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kLists] = s;
  }

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

  s = zsets_db_->Persist(key);
  if (s.ok()) {
    count++;
  } else if (!s.IsNotFound()) {
    is_corruption = true;
    (*type_status)[DataType::kLists] = s;
  }

  if (is_corruption) {
    return -1;
  } else {
    return count;
  }
}

std::map<DataType, int64_t> BlackWidow::TTL(const Slice& key,
                        std::map<DataType, Status>* type_status) {
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

  s = lists_db_->TTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kLists] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kLists] = -3;
    (*type_status)[DataType::kLists] = s;
  }

  s = sets_db_->TTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kSets] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kSets] = -3;
    (*type_status)[DataType::kSets] = s;
  }

  s = zsets_db_->TTL(key, &timestamp);
  if (s.ok() || s.IsNotFound()) {
    ret[DataType::kZSets] = timestamp;
  } else if (!s.IsNotFound()) {
    ret[DataType::kZSets] = -3;
    (*type_status)[DataType::kZSets] = s;
  }
  return ret;
}

//the sequence is kv, hash, list, zset, set
Status BlackWidow::Type(const std::string &key, std::string* type) {
  type->clear();

  Status s;
  std::string value;
  s = strings_db_->Get(key, &value);
  if (s.ok()) {
    *type = "string";
    return s;
  } else if (!s.IsNotFound()) {
    return s;
  }

  int32_t hashes_len = 0;
  s = hashes_db_->HLen(key, &hashes_len);
  if (s.ok() && hashes_len != 0) {
    *type = "hash";
    return s;
  } else if (!s.IsNotFound()) {
    return s;
  }

  uint64_t lists_len = 0;
  s = lists_db_->LLen(key, &lists_len);
  if (s.ok() && lists_len != 0) {
    *type = "list";
    return s;
  } else if (!s.IsNotFound()) {
    return s;
  }

  int32_t zsets_size = 0;
  s = zsets_db_->ZCard(key, &zsets_size);
  if (s.ok() && zsets_size != 0) {
    *type = "zset";
    return s;
  } else if (!s.IsNotFound()) {
    return s;
  }

  int32_t sets_size = 0;
  s = sets_db_->SCard(key, &sets_size);
  if (s.ok() && sets_size != 0) {
    *type = "set";
    return s;
  } else if (!s.IsNotFound()) {
    return s;
  }

  *type = "none";
  return Status::OK();
}

Status BlackWidow::Keys(const std::string& type,
                        const std::string& pattern,
                        std::vector<std::string>* keys) {
  Status s;
  if (type == "string") {
    s = strings_db_->ScanKeys(pattern, keys);
    if (!s.ok()) return s;
  } else if (type == "hash") {
    s = hashes_db_->ScanKeys(pattern, keys);
    if (!s.ok()) return s;
  } else if (type == "zset") {
    s = zsets_db_->ScanKeys(pattern, keys);
    if (!s.ok()) return s;
  } else if (type == "set") {
    s = sets_db_->ScanKeys(pattern, keys);
    if (!s.ok()) return s;
  } else if (type == "list") {
    s = lists_db_->ScanKeys(pattern, keys);
    if (!s.ok()) return s;
  } else {
    s = strings_db_->ScanKeys(pattern, keys);
    if (!s.ok()) return s;
    s = hashes_db_->ScanKeys(pattern, keys);
    if (!s.ok()) return s;
    s = zsets_db_->ScanKeys(pattern, keys);
    if (!s.ok()) return s;
    s = sets_db_->ScanKeys(pattern, keys);
    if (!s.ok()) return s;
    s = lists_db_->ScanKeys(pattern, keys);
    if (!s.ok()) return s;
  }
  return s;
}

void BlackWidow::ScanDatabase(const DataType& type) {

  switch (type) {
    case kStrings:
        strings_db_->ScanDatabase();
        break;
    case kHashes:
        hashes_db_->ScanDatabase();
        break;
    case kSets:
        sets_db_->ScanDatabase();
        break;
    case kZSets:
        zsets_db_->ScanDatabase();
        break;
    case kLists:
        lists_db_->ScanDatabase();
        break;
    case kAll:
        strings_db_->ScanDatabase();
        hashes_db_->ScanDatabase();
        sets_db_->ScanDatabase();
        zsets_db_->ScanDatabase();
        lists_db_->ScanDatabase();
        break;
  }
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

static void* StartBGThreadWrapper(void* arg) {
  BlackWidow* bw = reinterpret_cast<BlackWidow*>(arg);
  bw->RunBGTask();
  return NULL;
}

Status BlackWidow::StartBGThread() {
  int result = pthread_create(&bg_tasks_thread_id_, NULL, StartBGThreadWrapper, this);
  if (result != 0) {
    char msg[128];
    snprintf (msg, 128, "pthread create: %s", strerror(result));
    return Status::Corruption(msg);
  }
  return Status::OK();
}

Status BlackWidow::AddBGTask(const BGTask& bg_task) {
  bg_tasks_mutex_.Lock();
  bg_tasks_queue_.push(bg_task);
  bg_tasks_cond_var_.Signal();
  bg_tasks_mutex_.Unlock();
  return Status::OK();
}

Status BlackWidow::RunBGTask() {
  BGTask task;
  while (!bg_tasks_should_exit_) {

    bg_tasks_mutex_.Lock();
    while (bg_tasks_queue_.empty() && !bg_tasks_should_exit_) {
      bg_tasks_cond_var_.Wait();
    }

    if (!bg_tasks_queue_.empty()) {
      task = bg_tasks_queue_.front();
      bg_tasks_queue_.pop();
    }
    bg_tasks_mutex_.Unlock();

    if (bg_tasks_should_exit_) {
      return Status::Incomplete("bgtask return with bg_tasks_should_exit_ true");
    }

    if (task.operation == kCleanAll) {
      DoCompact(task.type);
    } else if (task.operation == kCompactKey) {
      CompactKey(task.type, task.argv);
    }
  }
  return Status::OK();
}

Status BlackWidow::Compact(const DataType& type, bool sync) {
  if (sync) {
    return DoCompact(type);
  } else {
    AddBGTask({type, kCleanAll});
  }
  return Status::OK();
}

Status BlackWidow::DoCompact(const DataType& type) {
  if (type != kAll
    && type != kStrings
    && type != kHashes
    && type != kSets
    && type != kZSets
    && type != kLists) {
    return Status::InvalidArgument("");
  }

  Status s;
  if (type == kStrings) {
    current_task_type_ = Operation::kCleanStrings;
    s = strings_db_->CompactRange(NULL, NULL);
  } else if (type == kHashes) {
    current_task_type_ = Operation::kCleanHashes;
    s = hashes_db_->CompactRange(NULL, NULL);
  } else if (type == kSets) {
    current_task_type_ = Operation::kCleanSets;
    s = sets_db_->CompactRange(NULL, NULL);
  } else if (type == kZSets) {
    current_task_type_ = Operation::kCleanZSets;
    s = zsets_db_->CompactRange(NULL, NULL);
  } else if (type == kLists) {
    current_task_type_ = Operation::kCleanLists;
    s = lists_db_->CompactRange(NULL, NULL);
  } else {
    current_task_type_ = Operation::kCleanAll;
    s = strings_db_->CompactRange(NULL, NULL);
    s = hashes_db_->CompactRange(NULL, NULL);
    s = sets_db_->CompactRange(NULL, NULL);
    s = zsets_db_->CompactRange(NULL, NULL);
    s = lists_db_->CompactRange(NULL, NULL);
  }
  current_task_type_ = Operation::kNone;
  return s;
}

Status BlackWidow::CompactKey(const DataType& type, const std::string& key) {

  Status s;
  std::string start_key, end_key;
  CalculateStartAndEndKey(key, &start_key, &end_key);
  if (type == kSets) {
    Slice slice_begin(start_key);
    Slice slice_end(end_key);
    s = sets_db_->CompactRange(&slice_begin, &slice_end);
  }
  return s;
}

std::string BlackWidow::GetCurrentTaskType() {
  int type = current_task_type_;
  switch (type) {
    case kCleanAll:
      return "All";
    case kCleanStrings:
      return "String";
    case kCleanHashes:
      return "Hash";
    case kCleanZSets:
      return "ZSet";
    case kCleanSets:
      return "Set";
    case kCleanLists:
      return "List";
    case kNone:
    default:
      return "No";
  }
}

Status BlackWidow::GetUsage(const std::string& type, uint64_t *result) {
  *result = 0;
  if (type == USAGE_TYPE_ALL || type == USAGE_TYPE_ROCKSDB || type == USAGE_TYPE_ROCKSDB_MEMTABLE) {
    *result += GetProperty("rocksdb.cur-size-all-mem-tables");
  }
  if (type == USAGE_TYPE_ALL || type == USAGE_TYPE_ROCKSDB || type == USAGE_TYPE_ROCKSDB_TABLE_READER) {
    *result += GetProperty("rocksdb.estimate-table-readers-mem");
  }
  if (type == USAGE_TYPE_ALL || type == USAGE_TYPE_NEMO) {
    //*result += GetLockUsage();
  }
  return Status::OK();
}

uint64_t BlackWidow::GetProperty(const std::string &property) {
  uint64_t result = 0;
  char *pEnd;
  std::string out;

  std::vector<Redis*> dbs = {strings_db_, lists_db_, hashes_db_, zsets_db_, sets_db_};
  for (auto iter = dbs.begin(); iter != dbs.end(); ++iter) {
    (*iter)->GetProperty(property, &out);
    result += std::strtoull(out.c_str(), &pEnd, 10);
  }
  return result;
}

Status BlackWidow::GetKeyNum(std::vector<uint64_t>* nums) {
  uint64_t num;
  if (scan_keynum_exit_) {
    scan_keynum_exit_ = false;
    return Status::Corruption("exit");
  }
  std::vector<Redis*> dbs = {strings_db_, lists_db_, hashes_db_, zsets_db_, sets_db_};
  for (auto iter = dbs.begin(); iter != dbs.end(); ++iter) {
    (*iter)->ScanKeyNum(&num);
    nums->push_back(num);
  }
  return Status::OK();
}

Status BlackWidow::StopScanKeyNum() {
  sleep(1);
  scan_keynum_exit_ = true;
  return Status::OK();
}

rocksdb::DB* BlackWidow::GetDBByType(const std::string& type) {
  if (type == STRINGS_DB) {
    return strings_db_->get_db();
  } else if (type == HASHES_DB) {
    return hashes_db_->get_db();
  } else if (type == LISTS_DB) {
    return lists_db_->get_db();
  } else if (type == SETS_DB) {
    return sets_db_->get_db();
  } else if (type == ZSETS_DB) {
    return zsets_db_->get_db();
  } else {
    return NULL;
  }
}

}  //  namespace blackwidow
