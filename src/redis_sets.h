//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_SETS_H_
#define SRC_REDIS_SETS_H_

#include <string>
#include <vector>
#include <unordered_set>

#include "slash/include/env.h"

#include "src/redis.h"
#include "src/custom_comparator.h"

#define SPOP_COMPACT_THRESHOLD_COUNT     500
#define SPOP_COMPACT_THRESHOLD_DURATION  1000         // 1000us

namespace blackwidow {

class RedisSets : public Redis {
  public:
    RedisSets();
    ~RedisSets();

    // Common Commands
    virtual Status Open(const rocksdb::Options& options,
                        const rocksdb::BlockBasedTableOptions& table_options,
                        const std::string& db_path) override;
    virtual Status CompactRange(const rocksdb::Slice* begin,
                                const rocksdb::Slice* end) override;
    virtual Status GetProperty(const std::string& property, uint64_t* out) override;
    virtual Status ScanKeyNum(uint64_t* num) override;
    virtual Status ScanKeys(const std::string& pattern,
                            std::vector<std::string>* keys) override;

    // Setes Commands
    Status SAdd(const Slice& key,
                const std::vector<std::string>& members, int32_t* ret);
    Status SCard(const Slice& key, int32_t* ret);
    Status SDiff(const std::vector<std::string>& keys,
                 std::vector<std::string>* members);
    Status SDiffstore(const Slice& destination,
                      const std::vector<std::string>& keys,
                      int32_t* ret);
    Status SInter(const std::vector<std::string>& keys,
                  std::vector<std::string>* members);
    Status SInterstore(const Slice& destination,
                       const std::vector<std::string>& keys,
                       int32_t* ret);
    Status SIsmember(const Slice& key, const Slice& member,
                     int32_t* ret);
    Status SMembers(const Slice& key,
                    std::vector<std::string>* members);
    Status SMove(const Slice& source, const Slice& destination,
                 const Slice& member, int32_t* ret);
    Status SPop(const Slice& key, std::string* member, bool* need_compact);
    Status SRandmember(const Slice& key, int32_t count,
                       std::vector<std::string>* members);
    Status SRem(const Slice& key, const std::vector<std::string>& members,
                int32_t* ret);
    Status SUnion(const std::vector<std::string>& keys,
                  std::vector<std::string>* members);
    Status SUnionstore(const Slice& destination,
                       const std::vector<std::string>& keys,
                       int32_t* ret);
    Status SScan(const Slice& key, int64_t cursor, const std::string& pattern,
                 int64_t count, std::vector<std::string>* members, int64_t* next_cursor);

    // Keys Commands
    virtual Status Expire(const Slice& key, int32_t ttl) override;
    virtual Status Del(const Slice& key) override;
    virtual bool Scan(const std::string& start_key, const std::string& pattern,
                      std::vector<std::string>* keys,
                      int64_t* count, std::string* next_key) override;
    virtual Status Expireat(const Slice& key, int32_t timestamp) override;
    virtual Status Persist(const Slice& key) override;
    virtual Status TTL(const Slice& key, int64_t* timestamp) override;

    // Iterate all data
    void ScanDatabase();

  private:
    std::vector<rocksdb::ColumnFamilyHandle*> handles_;

    // For compact in time after multiple spop
    slash::Mutex spop_counts_mutex_;
    BlackWidow::LRU<std::string, uint64_t> spop_counts_store_;
    Status ResetSpopCount(const std::string& key);
    Status AddAndGetSpopCount(const std::string& key, uint64_t* count);
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_SETS_H_
