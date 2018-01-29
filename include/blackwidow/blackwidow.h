//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_BLACKWIDOW_BLACKWIDOW_H_
#define INCLUDE_BLACKWIDOW_BLACKWIDOW_H_

#include <string>

#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace blackwidow {
using Options = rocksdb::Options;
using Status = rocksdb::Status;
using Slice = rocksdb::Slice;

class RedisStrings;
class RedisHashes;
class BlackWidow {
 public:
  BlackWidow();
  ~BlackWidow();

  // Just For Test, will be removed later;
  Status Compact();

  Status Open(const Options& options, const std::string& db_path);

  // Strings Commands
  struct KeyValue {
    Slice key;
    Slice value;
    bool operator < (const KeyValue& kv) const {
      return key.compare(kv.key) < 0;
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
  Status MGet(const std::vector<Slice>& keys, std::vector<std::string>* values);

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

  // Sets field in the hash stored at key to value. If key does not exist, a new
  // key holding a hash is created. If field already exists in the hash, it is
  // overwritten.
  Status HSet(const Slice& key, const Slice& field, const Slice& value,
              int32_t* res);

  // Returns the value associated with field in the hash stored at key.
  // the value associated with field, or nil when field is not present in the
  // hash or key does not exist.
  Status HGet(const Slice& key, const Slice& field, std::string* value);

  // Returns if field is an existing field in the hash stored at key.
  // Return Status::Ok() if the hash contains field.
  // Return Status::NotFound() if the hash does not contain field, or key does not exist.
  Status HExists(const Slice& key, const Slice& field);

  // Keys Commands
  enum DataType{
    STRINGS,
    HASHES,
    LISTS,
    SETS,
    ZSETS
  };

  // Set a timeout on key
  // return -1 operation exception errors happen in database
  // return >=0 success
  int Expire(const Slice& key, int32_t ttl, std::map<DataType, Status>* type_status);

  // Removes the specified keys
  // return -1 operation exception errors happen in database
  // return >=0 the number of keys that were removed
  int Del(const std::vector<Slice>& keys, std::map<DataType, Status>* type_status);

 private:
  RedisStrings* strings_db_;
  RedisHashes* hashes_db_;
};

}  //  namespace blackwidow
#endif  //  INCLUDE_BLACKWIDOW_BLACKWIDOW_H_
