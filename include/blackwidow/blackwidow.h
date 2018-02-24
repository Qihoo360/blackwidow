//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_BLACKWIDOW_BLACKWIDOW_H_
#define INCLUDE_BLACKWIDOW_BLACKWIDOW_H_

#include <string>
#include <map>
#include <vector>
#include <list>

#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace blackwidow {
using Options = rocksdb::Options;
using Status = rocksdb::Status;
using Slice = rocksdb::Slice;

class RedisStrings;
class RedisHashes;
class RedisSetes;
class MutexFactory;
class Mutex;
class BlackWidow {
 public:
  BlackWidow();
  ~BlackWidow();

  // Just For Test, will be removed later;
  Status Compact();

  Status Open(const Options& options, const std::string& db_path);

  Status GetStartKey(int64_t cursor, std::string* start_key);

  int64_t StoreAndGetCursor(int64_t cursor, const std::string& next_key);

  // Common
  template <typename T1, typename T2>
  struct LRU{
    int64_t max_size_;
    std::list<T1> list_;
    std::map<T1, T2> map_;
  };

  // Strings Commands
  struct KeyValue {
    std::string key;
    std::string value;
    bool operator < (const KeyValue& kv) const {
      return key < kv.key;
    }
  };

  // Set key to hold the string value. if key
  // already holds a value, it is overwritten
  Status Set(const Slice& key, const Slice& value);

  // Get the value of key. If the key does not exist
  // the special value nil is returned
  Status Get(const Slice& key, std::string* value);

  // Sets the given keys to their respective values
  // MSET replaces existing values with new values
  Status MSet(const std::vector<BlackWidow::KeyValue>& kvs);

  // Returns the values of all specified keys. For every key
  // that does not hold a string value or does not exist, the
  // special value nil is returned
  Status MGet(const std::vector<std::string>& keys,
              std::vector<std::string>* values);

  // Set key to hold string value if key does not exist
  // return 1 if the key was set
  // return 0 if the key was not set
  Status Setnx(const Slice& key, const Slice& value, int32_t* ret);

  // Set key to hold string value if key does not exist
  // return the length of the string after it was modified by the command
  Status Setrange(const Slice& key, int32_t offset,
                  const Slice& value, int32_t* ret);

  // If key already exists and is a string, this command appends the value at
  // the end of the string
  // return the length of the string after the append operation
  Status Append(const Slice& key, const Slice& value, int32_t* ret);

  // Count the number of set bits (population counting) in a string.
  // return the number of bits set to 1
  // note: if need to specified offset, set have_range to true
  Status BitCount(const Slice& key, int32_t start_offset, int32_t end_offset,
                  int32_t* ret, bool have_range);

  // Decrements the number stored at key by decrement
  // return the value of key after the decrement
  Status Decrby(const Slice& key, int64_t value, int64_t* ret);

  // Set key to hold the string value and set key to timeout after a given
  // number of seconds
  Status Setex(const Slice& key, const Slice& value, int32_t ttl);

  // Returns the length of the string value stored at key. An error
  // is returned when key holds a non-string value.
  Status Strlen(const Slice& key, int32_t* len);


  // Hashes Commands
  struct FieldValue {
    std::string field;
    std::string value;
  };

  // Sets field in the hash stored at key to value. If key does not exist, a new
  // key holding a hash is created. If field already exists in the hash, it is
  // overwritten.
  Status HSet(const Slice& key, const Slice& field, const Slice& value,
              int32_t* res);

  // Returns the value associated with field in the hash stored at key.
  // the value associated with field, or nil when field is not present in the
  // hash or key does not exist.
  Status HGet(const Slice& key, const Slice& field, std::string* value);

  // Sets the specified fields to their respective values in the hash stored at
  // key. This command overwrites any specified fields already existing in the
  // hash. If key does not exist, a new key holding a hash is created.
  Status HMSet(const Slice& key,
               const std::vector<BlackWidow::FieldValue>& fvs);

  // Returns the values associated with the specified fields in the hash stored
  // at key.
  // For every field that does not exist in the hash, a nil value is returned.
  // Because a non-existing keys are treated as empty hashes, running HMGET
  // against a non-existing key will return a list of nil values.
  Status HMGet(const Slice& key,
               const std::vector<std::string>& fields,
               std::vector<std::string>* values);

  // Returns all fields and values of the hash stored at key. In the returned
  // value, every field name is followed by its value, so the length of the
  // reply is twice the size of the hash.
  Status HGetall(const Slice& key,
                 std::vector<BlackWidow::FieldValue>* fvs);

  // Returns all field names in the hash stored at key.
  Status HKeys(const Slice& key,
               std::vector<std::string>* fields);

  // Returns all values in the hash stored at key.
  Status HVals(const Slice& key,
               std::vector<std::string>* values);

  // Sets field in the hash stored at key to value, only if field does not yet
  // exist. If key does not exist, a new key holding a hash is created. If field
  // already exists, this operation has no effect.
  Status HSetnx(const Slice& key, const Slice& field, const Slice& value,
                int32_t* ret);

  // Returns the number of fields contained in the hash stored at key.
  // Return 0 when key does not exist.
  Status HLen(const Slice& key, int32_t* ret);

  // Returns the string length of the value associated with field in the hash
  // stored at key. If the key or the field do not exist, 0 is returned.
  Status HStrlen(const Slice& key, const Slice& field, int32_t* len);

  // Returns if field is an existing field in the hash stored at key.
  // Return Status::Ok() if the hash contains field.
  // Return Status::NotFound() if the hash does not contain field,
  // or key does not exist.
  Status HExists(const Slice& key, const Slice& field);

  // Increments the number stored at field in the hash stored at key by
  // increment. If key does not exist, a new key holding a hash is created. If
  // field does not exist the value is set to 0 before the operation is
  // performed.
  Status HIncrby(const Slice& key, const Slice& field, int64_t value,
                 int64_t* ret);

  // Increment the specified field of a hash stored at key, and representing a
  // floating point number, by the specified increment. If the increment value
  // is negative, the result is to have the hash field value decremented instead
  // of incremented. If the field does not exist, it is set to 0 before
  // performing the operation. An error is returned if one of the following
  // conditions occur:
  //
  // The field contains a value of the wrong type (not a string).
  // The current field content or the specified increment are not parsable as a
  // double precision floating point number.
  Status HIncrbyfloat(const Slice& key, const Slice& field,
                      const Slice& by, std::string* new_value);

  // Removes the specified fields from the hash stored at key. Specified fields
  // that do not exist within this hash are ignored. If key does not exist, it
  // is treated as an empty hash and this command returns 0.
  Status HDel(const Slice& key, const std::vector<std::string>& fields,
              int32_t* ret);


  // Setes Commands

  // Add the specified members to the set stored at key. Specified members that
  // are already a member of this set are ignored. If key does not exist, a new
  // set is created before adding the specified members.
  Status SAdd(const Slice& key, const std::vector<std::string>& members,
              int32_t* ret);

  // Returns the set cardinality (number of elements) of the set stored at key.
  Status SCard(const Slice& key, int32_t* ret);


  // Keys Commands
  enum DataType {
    kStrings,
    kHashes,
    kLists,
    kZSets,
    kSetes
  };

  // Set a timeout on key
  // return -1 operation exception errors happen in database
  // return >=0 success
  int32_t Expire(const Slice& key, int32_t ttl,
                 std::map<DataType, Status>* type_status);

  // Removes the specified keys
  // return -1 operation exception errors happen in database
  // return >=0 the number of keys that were removed
  int64_t Del(const std::vector<Slice>& keys,
              std::map<DataType, Status>* type_status);

  // Iterate over a collection of elements
  // return an updated cursor that the user need to use as the cursor argument
  // in the next call
  int64_t Scan(int64_t cursor, const std::string& pattern,
               int64_t count, std::vector<std::string>* keys);

 private:
  RedisStrings* strings_db_;
  RedisHashes* hashes_db_;
  RedisSetes* setes_db_;

  MutexFactory* mutex_factory_;

  LRU<int64_t, std::string> cursors_store_;
  std::shared_ptr<Mutex> cursors_mutex_;
};

}  //  namespace blackwidow
#endif  //  INCLUDE_BLACKWIDOW_BLACKWIDOW_H_
