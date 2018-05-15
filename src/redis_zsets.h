//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_ZSETS_h
#define SRC_REDIS_ZSETS_h

#include <unordered_set>

#include "src/redis.h"
#include "src/custom_comparator.h"
#include "blackwidow/blackwidow.h"

namespace blackwidow {

class RedisZSets : public Redis {
 public:
  RedisZSets() = default;
  ~RedisZSets();

  // ZSets Commands
  Status ZAdd(const Slice& key,
              const std::vector<BlackWidow::ScoreMember>& score_members,
              int32_t* ret);
  Status ZCard(const Slice& key, int32_t* card);
  Status ZCount(const Slice& key,
                double min,
                double max,
                int32_t* ret);
  Status ZIncrby(const Slice& key,
                 const Slice& member,
                 double increment,
                 double* ret);
  Status ZRange(const Slice& key,
                int32_t start,
                int32_t stop,
                std::vector<BlackWidow::ScoreMember>* score_members);
  Status ZRank(const Slice& key,
               const Slice& member,
               int32_t* rank);
  Status ZRem(const Slice& key,
              std::vector<std::string> members,
              int32_t* ret);
  Status ZRemrangebyrank(const Slice& key,
                         int32_t start,
                         int32_t stop,
                         int32_t* ret);
  Status ZRemrangebyscore(const Slice& key,
                          double min,
                          double max,
                          int32_t* ret);
  Status ZRevrange(const Slice& key,
                   int32_t start,
                   int32_t stop,
                   std::vector<BlackWidow::ScoreMember>* score_members);
  Status ZRevrank(const Slice& key,
                  const Slice& member,
                  int32_t* rank);
  Status ZScore(const Slice& key, const Slice& member, double* score);

  // Common Commands
  virtual Status Open(const rocksdb::Options& options,
      const std::string& db_path) override;
  virtual Status CompactRange(const rocksdb::Slice* begin,
      const rocksdb::Slice* end) override;

  // Keys Commands
  virtual Status Expire(const Slice& key, int32_t ttl) override;
  virtual Status Del(const Slice& key) override;
  virtual bool Scan(const std::string& start_key, const std::string& pattern,
                    std::vector<std::string>* keys,
                    int64_t* count, std::string* next_key) override;
  virtual Status Expireat(const Slice& key, int32_t timestamp) override;
  virtual Status Persist(const Slice& key) override;
  virtual Status TTL(const Slice& key, int64_t* timestamp) override;

 private:
  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
};

} // namespace blackwidow
#endif  //  SRC_REDIS_ZSETS_h
