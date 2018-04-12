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
class RedisSets;
class RedisLists;
class HyperLogLog;
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

  enum BitOpType {
    kBitOpAnd = 1,
    kBitOpOr,
    kBitOpXor,
    kBitOpNot,
    kBitOpDefault
  };

  // Set key to hold the string value. if key
  // already holds a value, it is overwritten
  Status Set(const Slice& key, const Slice& value);

  // Get the value of key. If the key does not exist
  // the special value nil is returned
  Status Get(const Slice& key, std::string* value);

  // Atomically sets key to value and returns the old value stored at key
  // Returns an error when key exists but does not hold a string value.
  Status GetSet(const Slice& key, const Slice& value, std::string* old_value);

  // Sets or clears the bit at offset in the string value stored at key
  Status SetBit(const Slice& key, int64_t offset, int32_t value, int32_t* ret);

  // Returns the bit value at offset in the string value stored at key
  Status GetBit(const Slice& key, int64_t offset, int32_t* ret);

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

  // Sets the given keys to their respective values.
  // MSETNX will not perform any operation at all even
  // if just a single key already exists.
  Status MSetnx(const std::vector<BlackWidow::KeyValue>& kvs, int32_t* ret);

  // Set key to hold string value if key does not exist
  // return the length of the string after it was modified by the command
  Status Setrange(const Slice& key, int64_t start_offset,
                  const Slice& value, int32_t* ret);

  // Returns the substring of the string value stored at key,
  // determined by the offsets start and end (both are inclusive)
  Status Getrange(const Slice& key, int64_t start_offset, int64_t end_offset,
                  std::string* ret);

  // If key already exists and is a string, this command appends the value at
  // the end of the string
  // return the length of the string after the append operation
  Status Append(const Slice& key, const Slice& value, int32_t* ret);

  // Count the number of set bits (population counting) in a string.
  // return the number of bits set to 1
  // note: if need to specified offset, set have_range to true
  Status BitCount(const Slice& key, int64_t start_offset, int64_t end_offset,
                  int32_t* ret, bool have_offset);

  // Perform a bitwise operation between multiple keys
  // and store the result in the destination key
  Status BitOp(BitOpType op, const std::string& dest_key,
               const std::vector<std::string>& src_keys, int64_t* ret);

  // Return the position of the first bit set to 1 or 0 in a string
  // BitPos key 0
  Status BitPos(const Slice& key, int32_t bit, int64_t* ret);
  // BitPos key 0 [start]
  Status BitPos(const Slice& key, int32_t bit,
                int64_t start_offset, int64_t* ret);
  // BitPos key 0 [start] [end]
  Status BitPos(const Slice& key, int32_t bit,
                int64_t start_offset, int64_t end_offset,
                int64_t* ret);

  // Decrements the number stored at key by decrement
  // return the value of key after the decrement
  Status Decrby(const Slice& key, int64_t value, int64_t* ret);

  // Increments the number stored at key by increment.
  // If the key does not exist, it is set to 0 before performing the operation
  Status Incrby(const Slice& key, int64_t value, int64_t* ret);

  // Increment the string representing a floating point number
  // stored at key by the specified increment.
  Status Incrbyfloat(const Slice& key, const Slice& value, std::string* ret);

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


  // Sets Commands
  struct KeyVersion {
    std::string key;
    int32_t version;
  };

  // Add the specified members to the set stored at key. Specified members that
  // are already a member of this set are ignored. If key does not exist, a new
  // set is created before adding the specified members.
  Status SAdd(const Slice& key, const std::vector<std::string>& members,
              int32_t* ret);

  // Returns the set cardinality (number of elements) of the set stored at key.
  Status SCard(const Slice& key, int32_t* ret);

  // Returns the members of the set resulting from the difference between the
  // first set and all the successive sets.
  //
  // For example:
  //   key1 = {a, b, c, d}
  //   key2 = {c}
  //   key3 = {a, c, e}
  //   SDIFF key1 key2 key3  = {b, d}
  Status SDiff(const std::vector<std::string>& keys,
               std::vector<std::string>* members);

  // This command is equal to SDIFF, but instead of returning the resulting set,
  // it is stored in destination.
  // If destination already exists, it is overwritten.
  //
  // For example:
  //   destination = {};
  //   key1 = {a, b, c, d}
  //   key2 = {c}
  //   key3 = {a, c, e}
  //   SDIFFSTORE destination key1 key2 key3
  //   destination = {b, d}
  Status SDiffstore(const Slice& destination,
                    const std::vector<std::string>& keys,
                    int32_t* ret);

  // Returns the members of the set resulting from the intersection of all the
  // given sets.
  //
  // For example:
  //   key1 = {a, b, c, d}
  //   key2 = {c}
  //   key3 = {a, c, e}
  //   SINTER key1 key2 key3 = {c}
  Status SInter(const std::vector<std::string>& keys,
                std::vector<std::string>* members);

  // This command is equal to SINTER, but instead of returning the resulting
  // set, it is stored in destination.
  // If destination already exists, it is overwritten.
  //
  // For example:
  //   destination = {}
  //   key1 = {a, b, c, d}
  //   key2 = {a, c}
  //   key3 = {a, c, e}
  //   SINTERSTORE destination key1 key2 key3
  //   destination = {a, c}
  Status SInterstore(const Slice& destination,
                     const std::vector<std::string>& keys,
                     int32_t* ret);

  // Returns if member is a member of the set stored at key.
  Status SIsmember(const Slice& key, const Slice& member,
                   int32_t* ret);

  // Returns all the members of the set value stored at key.
  // This has the same effect as running SINTER with one argument key.
  Status SMembers(const Slice& key, std::vector<std::string>* members);

  // Remove the specified members from the set stored at key. Specified members
  // that are not a member of this set are ignored. If key does not exist, it is
  // treated as an empty set and this command returns 0.
  Status SRem(const Slice& key, const std::vector<std::string>& members,
              int32_t* ret);

  // Removes and returns one or more random elements from the set value store at
  // key.
  Status SPop(const Slice& key, int32_t count,
              std::vector<std::string>* members);

  // When called with just the key argument, return a random element from the
  // set value stored at key.
  // when called with the additional count argument, return an array of count
  // distinct elements if count is positive. If called with a negative count the
  // behavior changes and the command is allowed to return the same element
  // multiple times. In this case the number of returned elements is the
  // absolute value of the specified count
  Status SRandmembers(const Slice& key, int32_t count,
                      std::vector<std::string>* members);

  // Move member from the set at source to the set at destination. This
  // operation is atomic. In every given moment the element will appear to be a
  // member of source or destination for other clients.
  //
  // If the source set does not exist or does not contain the specified element,
  // no operation is performed and 0 is returned. Otherwise, the element is
  // removed from the source set and added to the destination set. When the
  // specified element already exists in the destination set, it is only removed
  // from the source set.
  Status SMove(const Slice& source, const Slice& destination,
               const Slice& member, int32_t* ret);

  // Returns the members of the set resulting from the union of all the given
  // sets.
  //
  // For example:
  //   key1 = {a, b, c, d}
  //   key2 = {c}
  //   key3 = {a, c, e}
  //   SUNION key1 key2 key3 = {a, b, c, d, e}
  Status SUnion(const std::vector<std::string>& keys,
                std::vector<std::string>* members);

  // This command is equal to SUNION, but instead of returning the resulting
  // set, it is stored in destination.
  // If destination already exists, it is overwritten.
  //
  // For example:
  //   key1 = {a, b}
  //   key2 = {c, d}
  //   key3 = {c, d, e}
  //   SUNIONSTORE destination key1 key2 key3
  //   destination = {a, b, c, d, e}
  Status SUnionstore(const Slice& destination,
                     const std::vector<std::string>& keys,
                     int32_t* ret);

  // Lists Commands

  // Insert all the specified values at the head of the list stored at key. If
  // key does not exist, it is created as empty list before performing the push
  // operations.
  Status LPush(const Slice& key, const std::vector<std::string>& values,
               uint64_t* ret);

  // Insert all the specified values at the tail of the list stored at key. If
  // key does not exist, it is created as empty list before performing the push
  // operation.
  Status RPush(const Slice& key, const std::vector<std::string>& values,
               uint64_t* ret);

  // Returns the specified elements of the list stored at key. The offsets start
  // and stop are zero-based indexes, with 0 being the first element of the list
  // (the head of the list), 1 being the next element and so on.
  Status LRange(const Slice& key, int64_t start, int64_t stop,
                std::vector<std::string>* ret);

  // Removes the first count occurrences of elements equal to value from the
  // list stored at key. The count argument influences the operation in the
  // following ways
  Status LTrim(const Slice& key, int64_t start, int64_t stop);

  // Keys Commands
  enum DataType {
    kStrings,
    kHashes,
    kLists,
    kZSets,
    kSets
  };

  // Note:
  // While any error happens, you need to check type_status for
  // the error message

  // Set a timeout on key
  // return -1 operation exception errors happen in database
  // return >=0 success
  int32_t Expire(const Slice& key, int32_t ttl,
                 std::map<DataType, Status>* type_status);

  // Removes the specified keys
  // return -1 operation exception errors happen in database
  // return >=0 the number of keys that were removed
  int64_t Del(const std::vector<std::string>& keys,
              std::map<DataType, Status>* type_status);

  // Iterate over a collection of elements
  // return an updated cursor that the user need to use as the cursor argument
  // in the next call
  int64_t Scan(int64_t cursor, const std::string& pattern,
               int64_t count, std::vector<std::string>* keys);

  // Returns if key exists.
  // return -1 operation exception errors happen in database
  // return >=0 the number of keys existing
  int64_t Exists(const std::vector<std::string>& keys,
                 std::map<DataType, Status>* type_status);

  // EXPIREAT has the same effect and semantic as EXPIRE, but instead of
  // specifying the number of seconds representing the TTL (time to live), it
  // takes an absolute Unix timestamp (seconds since January 1, 1970). A
  // timestamp in the past will delete the key immediately.
  // return -1 operation exception errors happen in database
  // return 0 if key does not exist
  // return >=1 if the timueout was set
  int32_t Expireat(const Slice& key, int32_t timestamp,
                   std::map<DataType, Status>* type_status);

  // Remove the existing timeout on key, turning the key from volatile (a key
  // with an expire set) to persistent (a key that will never expire as no
  // timeout is associated).
  // return -1 operation exception errors happen in database
  // return 0 if key does not exist or does not have an associated timeout
  // return >=1 if the timueout was set
  int32_t Persist(const Slice& key,
                  std::map<DataType, Status>* type_status);

  // Returns the remaining time to live of a key that has a timeout.
  // return -3 operation exception errors happen in database
  // return -2 if the key does not exist
  // return -1 if the key exists but has not associated expire
  // return > 0 TTL in seconds
  std::map<DataType, int64_t> TTL(const Slice& key,
                                  std::map<DataType, Status>* type_status);

  // HyperLogLog
  enum {
    kMaxKeys = 255,
    kPrecision = 17,
  };
  // Adds all the element arguments to the HyperLogLog data structure stored
  // at the variable name specified as first argument.
  Status PfAdd(const Slice& key, const std::vector<std::string>& values,
               bool* update);

  // When called with a single key, returns the approximated cardinality
  // computed by the HyperLogLog data structure stored at the specified
  // variable, which is 0 if the variable does not exist.
  Status PfCount(const std::vector<std::string>& keys, int64_t* result);

  // Merge multiple HyperLogLog values into an unique value that will
  // approximate the cardinality of the union of the observed Sets of the source
  // HyperLogLog structures.
  Status PfMerge(const std::vector<std::string>& keys);

 private:
  RedisStrings* strings_db_;
  RedisHashes* hashes_db_;
  RedisSets* sets_db_;
  RedisLists* lists_db_;

  MutexFactory* mutex_factory_;

  LRU<int64_t, std::string> cursors_store_;
  std::shared_ptr<Mutex> cursors_mutex_;
};

}  //  namespace blackwidow
#endif  //  INCLUDE_BLACKWIDOW_BLACKWIDOW_H_
