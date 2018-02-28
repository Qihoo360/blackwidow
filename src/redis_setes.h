//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_SETES_H_
#define SRC_REDIS_SETES_H_

#include <string>
#include <vector>
#include <unordered_set>

#include "src/redis.h"
#include "blackwidow/blackwidow.h"

namespace blackwidow {

class RedisSetes : public Redis {
 public:
    RedisSetes() = default;
    ~RedisSetes();

  // Setes Commands
  Status SAdd(const Slice& key,
              const std::vector<std::string>& members, int32_t* ret);
  Status SCard(const Slice& key, int32_t* ret);
  Status SDiff(const std::vector<std::string>& keys,
               std::vector<std::string>* members);
  Status SDiffstore(const Slice& destination,
                    const std::vector<std::string>& keys,
                    int32_t* ret);
  Status SIsmember(const Slice& key, const Slice& member,
                   int32_t* ret);
  Status SMembers(const Slice& key,
                  std::vector<std::string>* members);

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
  virtual Status TTL(const Slice& key, int32_t* timestamp) override;

 private:
  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_SETES_H_
