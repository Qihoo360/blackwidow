//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_LISTS_H_
#define SRC_REDIS_LISTS_H_

#include <string>
#include <vector>
#include <unordered_set>

#include "src/redis.h"
#include "src/custom_comparator.h"

namespace blackwidow {

class RedisLists : public Redis {
  public:
    RedisLists() = default;
    ~RedisLists();

    // Common commands
    virtual Status Open(const rocksdb::Options& options,
                        const rocksdb::BlockBasedTableOptions& table_options,
                        const std::string& db_path) override;
    virtual Status CompactRange(const rocksdb::Slice* begin,
                                const rocksdb::Slice* end) override;
    virtual Status GetProperty(const std::string& property, uint64_t* out) override;
    virtual Status ScanKeyNum(uint64_t* num) override;
    virtual Status ScanKeys(const std::string& pattern,
                            std::vector<std::string>* keys) override;


    // Lists commands;
    Status LIndex(const Slice& key, int64_t index, std::string* element);
    Status LInsert(const Slice& key, const BeforeOrAfter& before_or_after,
                   const std::string& pivot, const std::string& value, int64_t* ret);
    Status LLen(const Slice& key, uint64_t* len);
    Status LPop(const Slice& key, std::string* element);
    Status LPush(const Slice& key, const std::vector<std::string>& values,
                 uint64_t* ret);
    Status LPushx(const Slice& key, const Slice& value, uint64_t* len);
    Status LRange(const Slice& key, int64_t start, int64_t stop,
                  std::vector<std::string>* ret);
    Status LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* ret);
    Status LSet(const Slice& key, int64_t index, const Slice& value);
    Status LTrim(const Slice& key, int64_t start, int64_t stop);
    Status RPop(const Slice& key, std::string* element);
    Status RPoplpush(const Slice& source, const Slice& destination, std::string* element);
    Status RPush(const Slice& key, const std::vector<std::string>& values,
                 uint64_t* ret);
    Status RPushx(const Slice& key, const Slice& value, uint64_t* len);

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
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_LISTS_H_
