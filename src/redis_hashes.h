//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_HASHES_H_
#define SRC_REDIS_HASHES_H_

#include <string>
#include <vector>
#include <unordered_set>

#include "src/redis.h"

namespace blackwidow {

class RedisHashes : public Redis {
  public:
    RedisHashes() = default;
    ~RedisHashes();

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

    // Hashes Commands
    Status HDel(const Slice& key, const std::vector<std::string>& fields,
                int32_t* ret);
    Status HExists(const Slice& key, const Slice& field);
    Status HGet(const Slice& key, const Slice& field, std::string* value);
    Status HGetall(const Slice& key,
                   std::vector<FieldValue>* fvs);
    Status HIncrby(const Slice& key, const Slice& field, int64_t value,
                   int64_t* ret);
    Status HIncrbyfloat(const Slice& key, const Slice& field,
                        const Slice& by, std::string* new_value);
    Status HKeys(const Slice& key,
                 std::vector<std::string>* fields);
    Status HLen(const Slice& key, int32_t* ret);
    Status HMGet(const Slice& key, const std::vector<std::string>& fields,
                 std::vector<std::string>* values);
    Status HMSet(const Slice& key,
                 const std::vector<FieldValue>& fvs);
    Status HSet(const Slice& key, const Slice& field, const Slice& value,
                int32_t* ret);
    Status HSetnx(const Slice& key, const Slice& field, const Slice& value,
                  int32_t* ret);
    Status HVals(const Slice& key,
                 std::vector<std::string>* values);
    Status HStrlen(const Slice& key, const Slice& field, int32_t* len);
    Status HScan(const Slice& key, int64_t cursor, const std::string& pattern,
                 int64_t count, std::vector<FieldValue>* field_values, int64_t* next_cursor);
    Status HScanx(const Slice& key, const std::string start_field, const std::string& pattern,
                  int64_t count, std::vector<FieldValue>* field_values, std::string* next_field);


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
#endif  //  SRC_REDIS_HASHES_H_
