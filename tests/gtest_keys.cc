//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <thread>
#include <iostream>

#include "blackwidow/blackwidow.h"

using namespace blackwidow;

class KeysTest : public ::testing::Test {
 public:
  KeysTest() {
    std::string path = "./db/keys";
    if (access(path.c_str(), F_OK)) {
      mkdir(path.c_str(), 0755);
    }
    options.create_if_missing = true;
    s = db.Open(options, table_options, path);
  }
  virtual ~KeysTest() { }

  static void SetUpTestCase() { }
  static void TearDownTestCase() { }

  blackwidow::Options options;
  blackwidow::BlockBasedTableOptions table_options;
  blackwidow::BlackWidow db;
  blackwidow::Status s;
};

// Scan
// Note: This test needs to execute at first because all of the data is
// predetermined.
TEST_F(KeysTest, ScanTest) {

  int64_t cursor;
  int64_t next_cursor;
  int64_t del_num;
  int32_t int32_ret;
  uint64_t uint64_ret;
  std::vector<std::string> keys;
  std::vector<std::string> total_keys;
  std::vector<std::string> delete_keys;
  std::map<blackwidow::DataType, Status> type_status;

  // ***************** Group 1 Test *****************
  //String
  s = db.Set("GP1_SCAN_STRING_KEY1", "GP1_SCAN_STRING_VALUE1");
  s = db.Set("GP1_SCAN_STRING_KEY2", "GP1_SCAN_STRING_VALUE2");
  s = db.Set("GP1_SCAN_STRING_KEY3", "GP1_SCAN_STRING_VALUE3");

  //Hash
  s = db.HSet("GP1_SCAN_HASH_KEY1", "GP1_SCAN_HASH_FIELD1", "GP1_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP1_SCAN_HASH_KEY2", "GP1_SCAN_HASH_FIELD2", "GP1_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP1_SCAN_HASH_KEY3", "GP1_SCAN_HASH_FIELD3", "GP1_SCAN_HASH_VALUE3", &int32_ret);

  //Set
  s = db.SAdd("GP1_SCAN_SET_KEY1", {"GP1_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP1_SCAN_SET_KEY2", {"GP1_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP1_SCAN_SET_KEY3", {"GP1_SCAN_SET_MEMBER3"}, &int32_ret);

  //List
  s = db.LPush("GP1_SCAN_LIST_KEY1", {"GP1_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP1_SCAN_LIST_KEY2", {"GP1_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP1_SCAN_LIST_KEY3", {"GP1_SCAN_LIST_NODE3"}, &uint64_ret);

  //ZSet
  s = db.ZAdd("GP1_SCAN_ZSET_KEY1", {{1, "GP1_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP1_SCAN_ZSET_KEY2", {{1, "GP1_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP1_SCAN_ZSET_KEY3", {{1, "GP1_SCAN_LIST_MEMBER3"}}, &int32_ret);

  //Scan
  delete_keys.clear();
  keys.clear();
  cursor = db.Scan(0, "*", 3, &keys);
  ASSERT_EQ(cursor, 3);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_SCAN_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP1_SCAN_STRING_KEY2");
  ASSERT_EQ(keys[2], "GP1_SCAN_STRING_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(3, "*", 3, &keys);
  ASSERT_EQ(cursor, 6);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_SCAN_HASH_KEY1");
  ASSERT_EQ(keys[1], "GP1_SCAN_HASH_KEY2");
  ASSERT_EQ(keys[2], "GP1_SCAN_HASH_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(6, "*", 3, &keys);
  ASSERT_EQ(cursor, 9);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_SCAN_SET_KEY1");
  ASSERT_EQ(keys[1], "GP1_SCAN_SET_KEY2");
  ASSERT_EQ(keys[2], "GP1_SCAN_SET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(9, "*", 3, &keys);
  ASSERT_EQ(cursor, 12);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_SCAN_LIST_KEY1");
  ASSERT_EQ(keys[1], "GP1_SCAN_LIST_KEY2");
  ASSERT_EQ(keys[2], "GP1_SCAN_LIST_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(12, "*", 3, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP1_SCAN_ZSET_KEY1");
  ASSERT_EQ(keys[1], "GP1_SCAN_ZSET_KEY2");
  ASSERT_EQ(keys[2], "GP1_SCAN_ZSET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 2 Test *****************
  //String
  s = db.Set("GP2_SCAN_STRING_KEY1", "GP2_SCAN_STRING_VALUE1");
  s = db.Set("GP2_SCAN_STRING_KEY2", "GP2_SCAN_STRING_VALUE2");
  s = db.Set("GP2_SCAN_STRING_KEY3", "GP2_SCAN_STRING_VALUE3");

  //Hash
  s = db.HSet("GP2_SCAN_HASH_KEY1", "GP2_SCAN_HASH_FIELD1", "GP2_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP2_SCAN_HASH_KEY2", "GP2_SCAN_HASH_FIELD2", "GP2_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP2_SCAN_HASH_KEY3", "GP2_SCAN_HASH_FIELD3", "GP2_SCAN_HASH_VALUE3", &int32_ret);

  //Set
  s = db.SAdd("GP2_SCAN_SET_KEY1", {"GP2_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP2_SCAN_SET_KEY2", {"GP2_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP2_SCAN_SET_KEY3", {"GP2_SCAN_SET_MEMBER3"}, &int32_ret);

  //List
  s = db.LPush("GP2_SCAN_LIST_KEY1", {"GP2_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP2_SCAN_LIST_KEY2", {"GP2_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP2_SCAN_LIST_KEY3", {"GP2_SCAN_LIST_NODE3"}, &uint64_ret);

  //ZSet
  s = db.ZAdd("GP2_SCAN_ZSET_KEY1", {{1, "GP2_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP2_SCAN_ZSET_KEY2", {{1, "GP2_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP2_SCAN_ZSET_KEY3", {{1, "GP2_SCAN_LIST_MEMBER3"}}, &int32_ret);

  //Scan
  delete_keys.clear();
  keys.clear();
  cursor = db.Scan(0, "*", 2, &keys);
  ASSERT_EQ(cursor, 2);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP2_SCAN_STRING_KEY2");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(2, "*", 2, &keys);
  ASSERT_EQ(cursor, 4);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_STRING_KEY3");
  ASSERT_EQ(keys[1], "GP2_SCAN_HASH_KEY1");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(4, "*", 2, &keys);
  ASSERT_EQ(cursor, 6);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_HASH_KEY2");
  ASSERT_EQ(keys[1], "GP2_SCAN_HASH_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(6, "*", 2, &keys);
  ASSERT_EQ(cursor, 8);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_SET_KEY1");
  ASSERT_EQ(keys[1], "GP2_SCAN_SET_KEY2");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(8, "*", 2, &keys);
  ASSERT_EQ(cursor, 10);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_SET_KEY3");
  ASSERT_EQ(keys[1], "GP2_SCAN_LIST_KEY1");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(10, "*", 2, &keys);
  ASSERT_EQ(cursor, 12);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_LIST_KEY2");
  ASSERT_EQ(keys[1], "GP2_SCAN_LIST_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(12, "*", 2, &keys);
  ASSERT_EQ(cursor, 14);
  ASSERT_EQ(keys.size(), 2);
  ASSERT_EQ(keys[0], "GP2_SCAN_ZSET_KEY1");
  ASSERT_EQ(keys[1], "GP2_SCAN_ZSET_KEY2");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(14, "*", 2, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 1);
  ASSERT_EQ(keys[0], "GP2_SCAN_ZSET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 3 Test *****************
  //String
  s = db.Set("GP3_SCAN_STRING_KEY1", "GP3_SCAN_STRING_VALUE1");
  s = db.Set("GP3_SCAN_STRING_KEY2", "GP3_SCAN_STRING_VALUE2");
  s = db.Set("GP3_SCAN_STRING_KEY3", "GP3_SCAN_STRING_VALUE3");

  //Hash
  s = db.HSet("GP3_SCAN_HASH_KEY1", "GP3_SCAN_HASH_FIELD1", "GP3_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP3_SCAN_HASH_KEY2", "GP3_SCAN_HASH_FIELD2", "GP3_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP3_SCAN_HASH_KEY3", "GP3_SCAN_HASH_FIELD3", "GP3_SCAN_HASH_VALUE3", &int32_ret);

  //Set
  s = db.SAdd("GP3_SCAN_SET_KEY1", {"GP3_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP3_SCAN_SET_KEY2", {"GP3_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP3_SCAN_SET_KEY3", {"GP3_SCAN_SET_MEMBER3"}, &int32_ret);

  //List
  s = db.LPush("GP3_SCAN_LIST_KEY1", {"GP3_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP3_SCAN_LIST_KEY2", {"GP3_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP3_SCAN_LIST_KEY3", {"GP3_SCAN_LIST_NODE3"}, &uint64_ret);

  //ZSet
  s = db.ZAdd("GP3_SCAN_ZSET_KEY1", {{1, "GP3_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP3_SCAN_ZSET_KEY2", {{1, "GP3_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP3_SCAN_ZSET_KEY3", {{1, "GP3_SCAN_LIST_MEMBER3"}}, &int32_ret);

  //Scan
  delete_keys.clear();
  keys.clear();
  cursor = db.Scan(0, "*", 5, &keys);
  ASSERT_EQ(cursor, 5);
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP3_SCAN_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP3_SCAN_STRING_KEY2");
  ASSERT_EQ(keys[2], "GP3_SCAN_STRING_KEY3");
  ASSERT_EQ(keys[3], "GP3_SCAN_HASH_KEY1");
  ASSERT_EQ(keys[4], "GP3_SCAN_HASH_KEY2");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(5, "*", 5, &keys);
  ASSERT_EQ(cursor, 10);
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP3_SCAN_HASH_KEY3");
  ASSERT_EQ(keys[1], "GP3_SCAN_SET_KEY1");
  ASSERT_EQ(keys[2], "GP3_SCAN_SET_KEY2");
  ASSERT_EQ(keys[3], "GP3_SCAN_SET_KEY3");
  ASSERT_EQ(keys[4], "GP3_SCAN_LIST_KEY1");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  keys.clear();
  cursor = db.Scan(10, "*", 5, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP3_SCAN_LIST_KEY2");
  ASSERT_EQ(keys[1], "GP3_SCAN_LIST_KEY3");
  ASSERT_EQ(keys[2], "GP3_SCAN_ZSET_KEY1");
  ASSERT_EQ(keys[3], "GP3_SCAN_ZSET_KEY2");
  ASSERT_EQ(keys[4], "GP3_SCAN_ZSET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 4 Test *****************
  //String
  s = db.Set("GP4_SCAN_STRING_KEY1", "GP4_SCAN_STRING_VALUE1");
  s = db.Set("GP4_SCAN_STRING_KEY2", "GP4_SCAN_STRING_VALUE2");
  s = db.Set("GP4_SCAN_STRING_KEY3", "GP4_SCAN_STRING_VALUE3");

  //Hash
  s = db.HSet("GP4_SCAN_HASH_KEY1", "GP4_SCAN_HASH_FIELD1", "GP4_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP4_SCAN_HASH_KEY2", "GP4_SCAN_HASH_FIELD2", "GP4_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP4_SCAN_HASH_KEY3", "GP4_SCAN_HASH_FIELD3", "GP4_SCAN_HASH_VALUE3", &int32_ret);

  //Set
  s = db.SAdd("GP4_SCAN_SET_KEY1", {"GP4_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP4_SCAN_SET_KEY2", {"GP4_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP4_SCAN_SET_KEY3", {"GP4_SCAN_SET_MEMBER3"}, &int32_ret);

  //List
  s = db.LPush("GP4_SCAN_LIST_KEY1", {"GP4_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP4_SCAN_LIST_KEY2", {"GP4_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP4_SCAN_LIST_KEY3", {"GP4_SCAN_LIST_NODE3"}, &uint64_ret);

  //ZSet
  s = db.ZAdd("GP4_SCAN_ZSET_KEY1", {{1, "GP4_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP4_SCAN_ZSET_KEY2", {{1, "GP4_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP4_SCAN_ZSET_KEY3", {{1, "GP4_SCAN_LIST_MEMBER3"}}, &int32_ret);

  delete_keys.clear();
  keys.clear();
  cursor = db.Scan(0, "*", 15, &keys);
  ASSERT_EQ(cursor, 0);
  ASSERT_EQ(keys.size(), 15);
  ASSERT_EQ(keys[0], "GP4_SCAN_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP4_SCAN_STRING_KEY2");
  ASSERT_EQ(keys[2], "GP4_SCAN_STRING_KEY3");
  ASSERT_EQ(keys[3], "GP4_SCAN_HASH_KEY1");
  ASSERT_EQ(keys[4], "GP4_SCAN_HASH_KEY2");
  ASSERT_EQ(keys[5], "GP4_SCAN_HASH_KEY3");
  ASSERT_EQ(keys[6], "GP4_SCAN_SET_KEY1");
  ASSERT_EQ(keys[7], "GP4_SCAN_SET_KEY2");
  ASSERT_EQ(keys[8], "GP4_SCAN_SET_KEY3");
  ASSERT_EQ(keys[9], "GP4_SCAN_LIST_KEY1");
  ASSERT_EQ(keys[10], "GP4_SCAN_LIST_KEY2");
  ASSERT_EQ(keys[11], "GP4_SCAN_LIST_KEY3");
  ASSERT_EQ(keys[12], "GP4_SCAN_ZSET_KEY1");
  ASSERT_EQ(keys[13], "GP4_SCAN_ZSET_KEY2");
  ASSERT_EQ(keys[14], "GP4_SCAN_ZSET_KEY3");
  delete_keys.insert(delete_keys.end(), keys.begin(), keys.end());

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 5 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP5_SCAN_STRING_KEY1", "GP5_SCAN_STRING_VALUE1");
  s = db.Set("GP5_SCAN_STRING_KEY2", "GP5_SCAN_STRING_VALUE2");
  s = db.Set("GP5_SCAN_STRING_KEY3", "GP5_SCAN_STRING_VALUE3");
  delete_keys.push_back("GP5_SCAN_STRING_KEY1");
  delete_keys.push_back("GP5_SCAN_STRING_KEY2");
  delete_keys.push_back("GP5_SCAN_STRING_KEY3");

  //Hash
  s = db.HSet("GP5_SCAN_HASH_KEY1", "GP5_SCAN_HASH_FIELD1", "GP5_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP5_SCAN_HASH_KEY2", "GP5_SCAN_HASH_FIELD2", "GP5_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP5_SCAN_HASH_KEY3", "GP5_SCAN_HASH_FIELD3", "GP5_SCAN_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP5_SCAN_HASH_KEY1");
  delete_keys.push_back("GP5_SCAN_HASH_KEY2");
  delete_keys.push_back("GP5_SCAN_HASH_KEY3");

  //Set
  s = db.SAdd("GP5_SCAN_SET_KEY1", {"GP5_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP5_SCAN_SET_KEY2", {"GP5_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP5_SCAN_SET_KEY3", {"GP5_SCAN_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP5_SCAN_SET_KEY1");
  delete_keys.push_back("GP5_SCAN_SET_KEY2");
  delete_keys.push_back("GP5_SCAN_SET_KEY3");

  //List
  s = db.LPush("GP5_SCAN_LIST_KEY1", {"GP5_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP5_SCAN_LIST_KEY2", {"GP5_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP5_SCAN_LIST_KEY3", {"GP5_SCAN_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP5_SCAN_LIST_KEY1");
  delete_keys.push_back("GP5_SCAN_LIST_KEY2");
  delete_keys.push_back("GP5_SCAN_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP5_SCAN_ZSET_KEY1", {{1, "GP5_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP5_SCAN_ZSET_KEY2", {{1, "GP5_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP5_SCAN_ZSET_KEY3", {{1, "GP5_SCAN_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP5_SCAN_ZSET_KEY1");
  delete_keys.push_back("GP5_SCAN_ZSET_KEY2");
  delete_keys.push_back("GP5_SCAN_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  while ((next_cursor = db.Scan(cursor, "*_SET_*", 5, &keys)) != 0) {
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  }
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP5_SCAN_SET_KEY1");
  ASSERT_EQ(keys[1], "GP5_SCAN_SET_KEY2");
  ASSERT_EQ(keys[2], "GP5_SCAN_SET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 6 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP6_SCAN_STRING_KEY1", "GP6_SCAN_STRING_VALUE1");
  s = db.Set("GP6_SCAN_STRING_KEY2", "GP6_SCAN_STRING_VALUE2");
  s = db.Set("GP6_SCAN_STRING_KEY3", "GP6_SCAN_STRING_VALUE3");
  delete_keys.push_back("GP6_SCAN_STRING_KEY1");
  delete_keys.push_back("GP6_SCAN_STRING_KEY2");
  delete_keys.push_back("GP6_SCAN_STRING_KEY3");

  //Hash
  s = db.HSet("GP6_SCAN_HASH_KEY1", "GP6_SCAN_HASH_FIELD1", "GP6_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP6_SCAN_HASH_KEY2", "GP6_SCAN_HASH_FIELD2", "GP6_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP6_SCAN_HASH_KEY3", "GP6_SCAN_HASH_FIELD3", "GP6_SCAN_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP6_SCAN_HASH_KEY1");
  delete_keys.push_back("GP6_SCAN_HASH_KEY2");
  delete_keys.push_back("GP6_SCAN_HASH_KEY3");

  //Set
  s = db.SAdd("GP6_SCAN_SET_KEY1", {"GP6_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP6_SCAN_SET_KEY2", {"GP6_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP6_SCAN_SET_KEY3", {"GP6_SCAN_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP6_SCAN_SET_KEY1");
  delete_keys.push_back("GP6_SCAN_SET_KEY2");
  delete_keys.push_back("GP6_SCAN_SET_KEY3");

  //List
  s = db.LPush("GP6_SCAN_LIST_KEY1", {"GP6_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP6_SCAN_LIST_KEY2", {"GP6_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP6_SCAN_LIST_KEY3", {"GP6_SCAN_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP6_SCAN_LIST_KEY1");
  delete_keys.push_back("GP6_SCAN_LIST_KEY2");
  delete_keys.push_back("GP6_SCAN_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP6_SCAN_ZSET_KEY1", {{1, "GP6_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP6_SCAN_ZSET_KEY2", {{1, "GP6_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP6_SCAN_ZSET_KEY3", {{1, "GP6_SCAN_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP6_SCAN_ZSET_KEY1");
  delete_keys.push_back("GP6_SCAN_ZSET_KEY2");
  delete_keys.push_back("GP6_SCAN_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  while ((next_cursor = db.Scan(cursor, "*KEY1", 5, &keys)) != 0) {
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  }
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP6_SCAN_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP6_SCAN_HASH_KEY1");
  ASSERT_EQ(keys[2], "GP6_SCAN_SET_KEY1");
  ASSERT_EQ(keys[3], "GP6_SCAN_LIST_KEY1");
  ASSERT_EQ(keys[4], "GP6_SCAN_ZSET_KEY1");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 7 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP7_SCAN_STRING_KEY1", "GP7_SCAN_STRING_VALUE1");
  s = db.Set("GP7_SCAN_STRING_KEY2", "GP7_SCAN_STRING_VALUE2");
  s = db.Set("GP7_SCAN_STRING_KEY3", "GP7_SCAN_STRING_VALUE3");
  delete_keys.push_back("GP7_SCAN_STRING_KEY1");
  delete_keys.push_back("GP7_SCAN_STRING_KEY2");
  delete_keys.push_back("GP7_SCAN_STRING_KEY3");

  //Hash
  s = db.HSet("GP7_SCAN_HASH_KEY1", "GP7_SCAN_HASH_FIELD1", "GP7_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP7_SCAN_HASH_KEY2", "GP7_SCAN_HASH_FIELD2", "GP7_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP7_SCAN_HASH_KEY3", "GP7_SCAN_HASH_FIELD3", "GP7_SCAN_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP7_SCAN_HASH_KEY1");
  delete_keys.push_back("GP7_SCAN_HASH_KEY2");
  delete_keys.push_back("GP7_SCAN_HASH_KEY3");

  //Set
  s = db.SAdd("GP7_SCAN_SET_KEY1", {"GP7_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP7_SCAN_SET_KEY2", {"GP7_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP7_SCAN_SET_KEY3", {"GP7_SCAN_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP7_SCAN_SET_KEY1");
  delete_keys.push_back("GP7_SCAN_SET_KEY2");
  delete_keys.push_back("GP7_SCAN_SET_KEY3");

  //List
  s = db.LPush("GP7_SCAN_LIST_KEY1", {"GP7_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP7_SCAN_LIST_KEY2", {"GP7_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP7_SCAN_LIST_KEY3", {"GP7_SCAN_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP7_SCAN_LIST_KEY1");
  delete_keys.push_back("GP7_SCAN_LIST_KEY2");
  delete_keys.push_back("GP7_SCAN_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP7_SCAN_ZSET_KEY1", {{1, "GP7_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP7_SCAN_ZSET_KEY2", {{1, "GP7_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP7_SCAN_ZSET_KEY3", {{1, "GP7_SCAN_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP7_SCAN_ZSET_KEY1");
  delete_keys.push_back("GP7_SCAN_ZSET_KEY2");
  delete_keys.push_back("GP7_SCAN_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  while ((next_cursor = db.Scan(cursor, "*KEY2", 5, &keys)) != 0) {
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  }
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP7_SCAN_STRING_KEY2");
  ASSERT_EQ(keys[1], "GP7_SCAN_HASH_KEY2");
  ASSERT_EQ(keys[2], "GP7_SCAN_SET_KEY2");
  ASSERT_EQ(keys[3], "GP7_SCAN_LIST_KEY2");
  ASSERT_EQ(keys[4], "GP7_SCAN_ZSET_KEY2");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 8 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP8_SCAN_STRING_KEY1", "GP8_SCAN_STRING_VALUE1");
  s = db.Set("GP8_SCAN_STRING_KEY2", "GP8_SCAN_STRING_VALUE2");
  s = db.Set("GP8_SCAN_STRING_KEY3", "GP8_SCAN_STRING_VALUE3");
  delete_keys.push_back("GP8_SCAN_STRING_KEY1");
  delete_keys.push_back("GP8_SCAN_STRING_KEY2");
  delete_keys.push_back("GP8_SCAN_STRING_KEY3");

  //Hash
  s = db.HSet("GP8_SCAN_HASH_KEY1", "GP8_SCAN_HASH_FIELD1", "GP8_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP8_SCAN_HASH_KEY2", "GP8_SCAN_HASH_FIELD2", "GP8_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP8_SCAN_HASH_KEY3", "GP8_SCAN_HASH_FIELD3", "GP8_SCAN_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP8_SCAN_HASH_KEY1");
  delete_keys.push_back("GP8_SCAN_HASH_KEY2");
  delete_keys.push_back("GP8_SCAN_HASH_KEY3");

  //Set
  s = db.SAdd("GP8_SCAN_SET_KEY1", {"GP8_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP8_SCAN_SET_KEY2", {"GP8_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP8_SCAN_SET_KEY3", {"GP8_SCAN_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP8_SCAN_SET_KEY1");
  delete_keys.push_back("GP8_SCAN_SET_KEY2");
  delete_keys.push_back("GP8_SCAN_SET_KEY3");

  //List
  s = db.LPush("GP8_SCAN_LIST_KEY1", {"GP8_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP8_SCAN_LIST_KEY2", {"GP8_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP8_SCAN_LIST_KEY3", {"GP8_SCAN_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP8_SCAN_LIST_KEY1");
  delete_keys.push_back("GP8_SCAN_LIST_KEY2");
  delete_keys.push_back("GP8_SCAN_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP8_SCAN_ZSET_KEY1", {{1, "GP8_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP8_SCAN_ZSET_KEY2", {{1, "GP8_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP8_SCAN_ZSET_KEY3", {{1, "GP8_SCAN_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP8_SCAN_ZSET_KEY1");
  delete_keys.push_back("GP8_SCAN_ZSET_KEY2");
  delete_keys.push_back("GP8_SCAN_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  while ((next_cursor = db.Scan(cursor, "*KEY3", 5, &keys)) != 0) {
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  }
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP8_SCAN_STRING_KEY3");
  ASSERT_EQ(keys[1], "GP8_SCAN_HASH_KEY3");
  ASSERT_EQ(keys[2], "GP8_SCAN_SET_KEY3");
  ASSERT_EQ(keys[3], "GP8_SCAN_LIST_KEY3");
  ASSERT_EQ(keys[4], "GP8_SCAN_ZSET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 9 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP9_SCAN_STRING_KEY1", "GP9_SCAN_STRING_VALUE1");
  s = db.Set("GP9_SCAN_STRING_KEY2", "GP9_SCAN_STRING_VALUE2");
  s = db.Set("GP9_SCAN_STRING_KEY3", "GP9_SCAN_STRING_VALUE3");
  delete_keys.push_back("GP9_SCAN_STRING_KEY1");
  delete_keys.push_back("GP9_SCAN_STRING_KEY2");
  delete_keys.push_back("GP9_SCAN_STRING_KEY3");

  //Hash
  s = db.HSet("GP9_SCAN_HASH_KEY1", "GP9_SCAN_HASH_FIELD1", "GP9_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP9_SCAN_HASH_KEY2", "GP9_SCAN_HASH_FIELD2", "GP9_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP9_SCAN_HASH_KEY3", "GP9_SCAN_HASH_FIELD3", "GP9_SCAN_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP9_SCAN_HASH_KEY1");
  delete_keys.push_back("GP9_SCAN_HASH_KEY2");
  delete_keys.push_back("GP9_SCAN_HASH_KEY3");

  //Set
  s = db.SAdd("GP9_SCAN_SET_KEY1", {"GP9_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP9_SCAN_SET_KEY2", {"GP9_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP9_SCAN_SET_KEY3", {"GP9_SCAN_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP9_SCAN_SET_KEY1");
  delete_keys.push_back("GP9_SCAN_SET_KEY2");
  delete_keys.push_back("GP9_SCAN_SET_KEY3");

  //List
  s = db.LPush("GP9_SCAN_LIST_KEY1", {"GP9_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP9_SCAN_LIST_KEY2", {"GP9_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP9_SCAN_LIST_KEY3", {"GP9_SCAN_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP9_SCAN_LIST_KEY1");
  delete_keys.push_back("GP9_SCAN_LIST_KEY2");
  delete_keys.push_back("GP9_SCAN_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP9_SCAN_ZSET_KEY1", {{1, "GP9_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP9_SCAN_ZSET_KEY2", {{1, "GP9_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP9_SCAN_ZSET_KEY3", {{1, "GP9_SCAN_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP9_SCAN_ZSET_KEY1");
  delete_keys.push_back("GP9_SCAN_ZSET_KEY2");
  delete_keys.push_back("GP9_SCAN_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  while ((next_cursor = db.Scan(cursor, "GP9*", 5, &keys)) != 0) {
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  }
  ASSERT_EQ(keys.size(), 15);
  ASSERT_EQ(keys[0], "GP9_SCAN_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP9_SCAN_STRING_KEY2");
  ASSERT_EQ(keys[2], "GP9_SCAN_STRING_KEY3");
  ASSERT_EQ(keys[3], "GP9_SCAN_HASH_KEY1");
  ASSERT_EQ(keys[4], "GP9_SCAN_HASH_KEY2");
  ASSERT_EQ(keys[5], "GP9_SCAN_HASH_KEY3");
  ASSERT_EQ(keys[6], "GP9_SCAN_SET_KEY1");
  ASSERT_EQ(keys[7], "GP9_SCAN_SET_KEY2");
  ASSERT_EQ(keys[8], "GP9_SCAN_SET_KEY3");
  ASSERT_EQ(keys[9], "GP9_SCAN_LIST_KEY1");
  ASSERT_EQ(keys[10], "GP9_SCAN_LIST_KEY2");
  ASSERT_EQ(keys[11], "GP9_SCAN_LIST_KEY3");
  ASSERT_EQ(keys[12], "GP9_SCAN_ZSET_KEY1");
  ASSERT_EQ(keys[13], "GP9_SCAN_ZSET_KEY2");
  ASSERT_EQ(keys[14], "GP9_SCAN_ZSET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 10 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP10_SCAN_STRING_KEY1", "GP10_SCAN_STRING_VALUE1");
  s = db.Set("GP10_SCAN_STRING_KEY2", "GP10_SCAN_STRING_VALUE2");
  s = db.Set("GP10_SCAN_STRING_KEY3", "GP10_SCAN_STRING_VALUE3");
  delete_keys.push_back("GP10_SCAN_STRING_KEY1");
  delete_keys.push_back("GP10_SCAN_STRING_KEY2");
  delete_keys.push_back("GP10_SCAN_STRING_KEY3");

  //Hash
  s = db.HSet("GP10_SCAN_HASH_KEY1", "GP10_SCAN_HASH_FIELD1", "GP10_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP10_SCAN_HASH_KEY2", "GP10_SCAN_HASH_FIELD2", "GP10_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP10_SCAN_HASH_KEY3", "GP10_SCAN_HASH_FIELD3", "GP10_SCAN_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP10_SCAN_HASH_KEY1");
  delete_keys.push_back("GP10_SCAN_HASH_KEY2");
  delete_keys.push_back("GP10_SCAN_HASH_KEY3");

  //Set
  s = db.SAdd("GP10_SCAN_SET_KEY1", {"GP10_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP10_SCAN_SET_KEY2", {"GP10_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP10_SCAN_SET_KEY3", {"GP10_SCAN_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP10_SCAN_SET_KEY1");
  delete_keys.push_back("GP10_SCAN_SET_KEY2");
  delete_keys.push_back("GP10_SCAN_SET_KEY3");

  //List
  s = db.LPush("GP10_SCAN_LIST_KEY1", {"GP10_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP10_SCAN_LIST_KEY2", {"GP10_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP10_SCAN_LIST_KEY3", {"GP10_SCAN_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP10_SCAN_LIST_KEY1");
  delete_keys.push_back("GP10_SCAN_LIST_KEY2");
  delete_keys.push_back("GP10_SCAN_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP10_SCAN_ZSET_KEY1", {{1, "GP10_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP10_SCAN_ZSET_KEY2", {{1, "GP10_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP10_SCAN_ZSET_KEY3", {{1, "GP10_SCAN_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP10_SCAN_ZSET_KEY1");
  delete_keys.push_back("GP10_SCAN_ZSET_KEY2");
  delete_keys.push_back("GP10_SCAN_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  while ((next_cursor = db.Scan(cursor, "GP10_SCAN_STRING_*", 5, &keys)) != 0) {
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  }
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP10_SCAN_STRING_KEY1");
  ASSERT_EQ(keys[1], "GP10_SCAN_STRING_KEY2");
  ASSERT_EQ(keys[2], "GP10_SCAN_STRING_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 11 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP11_SCAN_STRING_KEY1", "GP11_SCAN_STRING_VALUE1");
  s = db.Set("GP11_SCAN_STRING_KEY2", "GP11_SCAN_STRING_VALUE2");
  s = db.Set("GP11_SCAN_STRING_KEY3", "GP11_SCAN_STRING_VALUE3");
  delete_keys.push_back("GP11_SCAN_STRING_KEY1");
  delete_keys.push_back("GP11_SCAN_STRING_KEY2");
  delete_keys.push_back("GP11_SCAN_STRING_KEY3");

  //Hash
  s = db.HSet("GP11_SCAN_HASH_KEY1", "GP11_SCAN_HASH_FIELD1", "GP11_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP11_SCAN_HASH_KEY2", "GP11_SCAN_HASH_FIELD2", "GP11_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP11_SCAN_HASH_KEY3", "GP11_SCAN_HASH_FIELD3", "GP11_SCAN_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP11_SCAN_HASH_KEY1");
  delete_keys.push_back("GP11_SCAN_HASH_KEY2");
  delete_keys.push_back("GP11_SCAN_HASH_KEY3");

  //Set
  s = db.SAdd("GP11_SCAN_SET_KEY1", {"GP11_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP11_SCAN_SET_KEY2", {"GP11_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP11_SCAN_SET_KEY3", {"GP11_SCAN_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP11_SCAN_SET_KEY1");
  delete_keys.push_back("GP11_SCAN_SET_KEY2");
  delete_keys.push_back("GP11_SCAN_SET_KEY3");

  //List
  s = db.LPush("GP11_SCAN_LIST_KEY1", {"GP11_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP11_SCAN_LIST_KEY2", {"GP11_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP11_SCAN_LIST_KEY3", {"GP11_SCAN_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP11_SCAN_LIST_KEY1");
  delete_keys.push_back("GP11_SCAN_LIST_KEY2");
  delete_keys.push_back("GP11_SCAN_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP11_SCAN_ZSET_KEY1", {{1, "GP11_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP11_SCAN_ZSET_KEY2", {{1, "GP11_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP11_SCAN_ZSET_KEY3", {{1, "GP11_SCAN_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP11_SCAN_ZSET_KEY1");
  delete_keys.push_back("GP11_SCAN_ZSET_KEY2");
  delete_keys.push_back("GP11_SCAN_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  while ((next_cursor = db.Scan(cursor, "GP11_SCAN_SET_*", 5, &keys)) != 0) {
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  }
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP11_SCAN_SET_KEY1");
  ASSERT_EQ(keys[1], "GP11_SCAN_SET_KEY2");
  ASSERT_EQ(keys[2], "GP11_SCAN_SET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 12 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP12_SCAN_STRING_KEY1", "GP12_SCAN_STRING_VALUE1");
  s = db.Set("GP12_SCAN_STRING_KEY2", "GP12_SCAN_STRING_VALUE2");
  s = db.Set("GP12_SCAN_STRING_KEY3", "GP12_SCAN_STRING_VALUE3");
  delete_keys.push_back("GP12_SCAN_STRING_KEY1");
  delete_keys.push_back("GP12_SCAN_STRING_KEY2");
  delete_keys.push_back("GP12_SCAN_STRING_KEY3");

  //Hash
  s = db.HSet("GP12_SCAN_HASH_KEY1", "GP12_SCAN_HASH_FIELD1", "GP12_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP12_SCAN_HASH_KEY2", "GP12_SCAN_HASH_FIELD2", "GP12_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP12_SCAN_HASH_KEY3", "GP12_SCAN_HASH_FIELD3", "GP12_SCAN_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP12_SCAN_HASH_KEY1");
  delete_keys.push_back("GP12_SCAN_HASH_KEY2");
  delete_keys.push_back("GP12_SCAN_HASH_KEY3");

  //Set
  s = db.SAdd("GP12_SCAN_SET_KEY1", {"GP12_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP12_SCAN_SET_KEY2", {"GP12_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP12_SCAN_SET_KEY3", {"GP12_SCAN_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP12_SCAN_SET_KEY1");
  delete_keys.push_back("GP12_SCAN_SET_KEY2");
  delete_keys.push_back("GP12_SCAN_SET_KEY3");

  //List
  s = db.LPush("GP12_SCAN_LIST_KEY1", {"GP12_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP12_SCAN_LIST_KEY2", {"GP12_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP12_SCAN_LIST_KEY3", {"GP12_SCAN_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP12_SCAN_LIST_KEY1");
  delete_keys.push_back("GP12_SCAN_LIST_KEY2");
  delete_keys.push_back("GP12_SCAN_LIST_KEY3");

  //ZSet
  s = db.ZAdd("GP12_SCAN_ZSET_KEY1", {{1, "GP12_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP12_SCAN_ZSET_KEY2", {{1, "GP12_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP12_SCAN_ZSET_KEY3", {{1, "GP12_SCAN_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP12_SCAN_ZSET_KEY1");
  delete_keys.push_back("GP12_SCAN_ZSET_KEY2");
  delete_keys.push_back("GP12_SCAN_ZSET_KEY3");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  while ((next_cursor = db.Scan(cursor, "GP12_SCAN_ZSET_*", 5, &keys)) != 0) {
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  }
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(keys[0], "GP12_SCAN_ZSET_KEY1");
  ASSERT_EQ(keys[1], "GP12_SCAN_ZSET_KEY2");
  ASSERT_EQ(keys[2], "GP12_SCAN_ZSET_KEY3");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 13 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP13_KEY1_SCAN_STRING", "GP13_SCAN_STRING_VALUE1");
  s = db.Set("GP13_KEY2_SCAN_STRING", "GP13_SCAN_STRING_VALUE2");
  s = db.Set("GP13_KEY3_SCAN_STRING", "GP13_SCAN_STRING_VALUE3");
  delete_keys.push_back("GP13_KEY1_SCAN_STRING");
  delete_keys.push_back("GP13_KEY2_SCAN_STRING");
  delete_keys.push_back("GP13_KEY3_SCAN_STRING");

  //Hash
  s = db.HSet("GP13_KEY1_SCAN_HASH", "GP13_SCAN_HASH_FIELD1", "GP13_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP13_KEY2_SCAN_HASH", "GP13_SCAN_HASH_FIELD2", "GP13_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP13_KEY3_SCAN_HASH", "GP13_SCAN_HASH_FIELD3", "GP13_SCAN_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP13_KEY1_SCAN_HASH");
  delete_keys.push_back("GP13_KEY2_SCAN_HASH");
  delete_keys.push_back("GP13_KEY3_SCAN_HASH");

  //Set
  s = db.SAdd("GP13_KEY1_SCAN_SET", {"GP13_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP13_KEY2_SCAN_SET", {"GP13_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP13_KEY3_SCAN_SET", {"GP13_SCAN_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP13_KEY1_SCAN_SET");
  delete_keys.push_back("GP13_KEY2_SCAN_SET");
  delete_keys.push_back("GP13_KEY3_SCAN_SET");

  //List
  s = db.LPush("GP13_KEY1_SCAN_LIST", {"GP13_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP13_KEY2_SCAN_LIST", {"GP13_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP13_KEY3_SCAN_LIST", {"GP13_SCAN_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP13_KEY1_SCAN_LIST");
  delete_keys.push_back("GP13_KEY2_SCAN_LIST");
  delete_keys.push_back("GP13_KEY3_SCAN_LIST");

  //ZSet
  s = db.ZAdd("GP13_KEY1_SCAN_ZSET", {{1, "GP13_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP13_KEY2_SCAN_ZSET", {{1, "GP13_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP13_KEY3_SCAN_ZSET", {{1, "GP13_SCAN_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP13_KEY1_SCAN_ZSET");
  delete_keys.push_back("GP13_KEY2_SCAN_ZSET");
  delete_keys.push_back("GP13_KEY3_SCAN_ZSET");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  while ((next_cursor = db.Scan(cursor, "GP13_KEY1_SCAN_*", 1, &keys)) != 0) {
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  }
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP13_KEY1_SCAN_STRING");
  ASSERT_EQ(keys[1], "GP13_KEY1_SCAN_HASH");
  ASSERT_EQ(keys[2], "GP13_KEY1_SCAN_SET");
  ASSERT_EQ(keys[3], "GP13_KEY1_SCAN_LIST");
  ASSERT_EQ(keys[4], "GP13_KEY1_SCAN_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 14 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP14_KEY1_SCAN_STRING", "GP14_SCAN_STRING_VALUE1");
  s = db.Set("GP14_KEY2_SCAN_STRING", "GP14_SCAN_STRING_VALUE2");
  s = db.Set("GP14_KEY3_SCAN_STRING", "GP14_SCAN_STRING_VALUE3");
  delete_keys.push_back("GP14_KEY1_SCAN_STRING");
  delete_keys.push_back("GP14_KEY2_SCAN_STRING");
  delete_keys.push_back("GP14_KEY3_SCAN_STRING");

  //Hash
  s = db.HSet("GP14_KEY1_SCAN_HASH", "GP14_SCAN_HASH_FIELD1", "GP14_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP14_KEY2_SCAN_HASH", "GP14_SCAN_HASH_FIELD2", "GP14_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP14_KEY3_SCAN_HASH", "GP14_SCAN_HASH_FIELD3", "GP14_SCAN_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP14_KEY1_SCAN_HASH");
  delete_keys.push_back("GP14_KEY2_SCAN_HASH");
  delete_keys.push_back("GP14_KEY3_SCAN_HASH");

  //Set
  s = db.SAdd("GP14_KEY1_SCAN_SET", {"GP14_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP14_KEY2_SCAN_SET", {"GP14_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP14_KEY3_SCAN_SET", {"GP14_SCAN_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP14_KEY1_SCAN_SET");
  delete_keys.push_back("GP14_KEY2_SCAN_SET");
  delete_keys.push_back("GP14_KEY3_SCAN_SET");

  //List
  s = db.LPush("GP14_KEY1_SCAN_LIST", {"GP14_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP14_KEY2_SCAN_LIST", {"GP14_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP14_KEY3_SCAN_LIST", {"GP14_SCAN_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP14_KEY1_SCAN_LIST");
  delete_keys.push_back("GP14_KEY2_SCAN_LIST");
  delete_keys.push_back("GP14_KEY3_SCAN_LIST");

  //ZSet
  s = db.ZAdd("GP14_KEY1_SCAN_ZSET", {{1, "GP14_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP14_KEY2_SCAN_ZSET", {{1, "GP14_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP14_KEY3_SCAN_ZSET", {{1, "GP14_SCAN_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP14_KEY1_SCAN_ZSET");
  delete_keys.push_back("GP14_KEY2_SCAN_ZSET");
  delete_keys.push_back("GP14_KEY3_SCAN_ZSET");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  while ((next_cursor = db.Scan(cursor, "GP14_KEY1_SCAN_*", 5, &keys)) != 0) {
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  }
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP14_KEY1_SCAN_STRING");
  ASSERT_EQ(keys[1], "GP14_KEY1_SCAN_HASH");
  ASSERT_EQ(keys[2], "GP14_KEY1_SCAN_SET");
  ASSERT_EQ(keys[3], "GP14_KEY1_SCAN_LIST");
  ASSERT_EQ(keys[4], "GP14_KEY1_SCAN_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 15 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP15_KEY1_SCAN_STRING", "GP15_SCAN_STRING_VALUE1");
  s = db.Set("GP15_KEY2_SCAN_STRING", "GP15_SCAN_STRING_VALUE2");
  s = db.Set("GP15_KEY3_SCAN_STRING", "GP15_SCAN_STRING_VALUE3");
  delete_keys.push_back("GP15_KEY1_SCAN_STRING");
  delete_keys.push_back("GP15_KEY2_SCAN_STRING");
  delete_keys.push_back("GP15_KEY3_SCAN_STRING");

  //Hash
  s = db.HSet("GP15_KEY1_SCAN_HASH", "GP15_SCAN_HASH_FIELD1", "GP15_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP15_KEY2_SCAN_HASH", "GP15_SCAN_HASH_FIELD2", "GP15_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP15_KEY3_SCAN_HASH", "GP15_SCAN_HASH_FIELD3", "GP15_SCAN_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP15_KEY1_SCAN_HASH");
  delete_keys.push_back("GP15_KEY2_SCAN_HASH");
  delete_keys.push_back("GP15_KEY3_SCAN_HASH");

  //Set
  s = db.SAdd("GP15_KEY1_SCAN_SET", {"GP15_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP15_KEY2_SCAN_SET", {"GP15_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP15_KEY3_SCAN_SET", {"GP15_SCAN_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP15_KEY1_SCAN_SET");
  delete_keys.push_back("GP15_KEY2_SCAN_SET");
  delete_keys.push_back("GP15_KEY3_SCAN_SET");

  //List
  s = db.LPush("GP15_KEY1_SCAN_LIST", {"GP15_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP15_KEY2_SCAN_LIST", {"GP15_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP15_KEY3_SCAN_LIST", {"GP15_SCAN_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP15_KEY1_SCAN_LIST");
  delete_keys.push_back("GP15_KEY2_SCAN_LIST");
  delete_keys.push_back("GP15_KEY3_SCAN_LIST");

  //ZSet
  s = db.ZAdd("GP15_KEY1_SCAN_ZSET", {{1, "GP15_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP15_KEY2_SCAN_ZSET", {{1, "GP15_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP15_KEY3_SCAN_ZSET", {{1, "GP15_SCAN_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP15_KEY1_SCAN_ZSET");
  delete_keys.push_back("GP15_KEY2_SCAN_ZSET");
  delete_keys.push_back("GP15_KEY3_SCAN_ZSET");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  while ((next_cursor = db.Scan(cursor, "GP15_KEY1_SCAN_*", 5, &keys)) != 0) {
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  }
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP15_KEY1_SCAN_STRING");
  ASSERT_EQ(keys[1], "GP15_KEY1_SCAN_HASH");
  ASSERT_EQ(keys[2], "GP15_KEY1_SCAN_SET");
  ASSERT_EQ(keys[3], "GP15_KEY1_SCAN_LIST");
  ASSERT_EQ(keys[4], "GP15_KEY1_SCAN_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);


  // ***************** Group 16 Test *****************
  delete_keys.clear();
  //String
  s = db.Set("GP16_KEY1_SCAN_STRING", "GP16_SCAN_STRING_VALUE1");
  s = db.Set("GP16_KEY2_SCAN_STRING", "GP16_SCAN_STRING_VALUE2");
  s = db.Set("GP16_KEY3_SCAN_STRING", "GP16_SCAN_STRING_VALUE3");
  delete_keys.push_back("GP16_KEY1_SCAN_STRING");
  delete_keys.push_back("GP16_KEY2_SCAN_STRING");
  delete_keys.push_back("GP16_KEY3_SCAN_STRING");

  //Hash
  s = db.HSet("GP16_KEY1_SCAN_HASH", "GP16_SCAN_HASH_FIELD1", "GP16_SCAN_HASH_VALUE1", &int32_ret);
  s = db.HSet("GP16_KEY2_SCAN_HASH", "GP16_SCAN_HASH_FIELD2", "GP16_SCAN_HASH_VALUE2", &int32_ret);
  s = db.HSet("GP16_KEY3_SCAN_HASH", "GP16_SCAN_HASH_FIELD3", "GP16_SCAN_HASH_VALUE3", &int32_ret);
  delete_keys.push_back("GP16_KEY1_SCAN_HASH");
  delete_keys.push_back("GP16_KEY2_SCAN_HASH");
  delete_keys.push_back("GP16_KEY3_SCAN_HASH");

  //Set
  s = db.SAdd("GP16_KEY1_SCAN_SET", {"GP16_SCAN_SET_MEMBER1"}, &int32_ret);
  s = db.SAdd("GP16_KEY2_SCAN_SET", {"GP16_SCAN_SET_MEMBER2"}, &int32_ret);
  s = db.SAdd("GP16_KEY3_SCAN_SET", {"GP16_SCAN_SET_MEMBER3"}, &int32_ret);
  delete_keys.push_back("GP16_KEY1_SCAN_SET");
  delete_keys.push_back("GP16_KEY2_SCAN_SET");
  delete_keys.push_back("GP16_KEY3_SCAN_SET");

  //List
  s = db.LPush("GP16_KEY1_SCAN_LIST", {"GP16_SCAN_LIST_NODE1"}, &uint64_ret);
  s = db.LPush("GP16_KEY2_SCAN_LIST", {"GP16_SCAN_LIST_NODE2"}, &uint64_ret);
  s = db.LPush("GP16_KEY3_SCAN_LIST", {"GP16_SCAN_LIST_NODE3"}, &uint64_ret);
  delete_keys.push_back("GP16_KEY1_SCAN_LIST");
  delete_keys.push_back("GP16_KEY2_SCAN_LIST");
  delete_keys.push_back("GP16_KEY3_SCAN_LIST");

  //ZSet
  s = db.ZAdd("GP16_KEY1_SCAN_ZSET", {{1, "GP16_SCAN_LIST_MEMBER1"}}, &int32_ret);
  s = db.ZAdd("GP16_KEY2_SCAN_ZSET", {{1, "GP16_SCAN_LIST_MEMBER2"}}, &int32_ret);
  s = db.ZAdd("GP16_KEY3_SCAN_ZSET", {{1, "GP16_SCAN_LIST_MEMBER3"}}, &int32_ret);
  delete_keys.push_back("GP16_KEY1_SCAN_ZSET");
  delete_keys.push_back("GP16_KEY2_SCAN_ZSET");
  delete_keys.push_back("GP16_KEY3_SCAN_ZSET");

  cursor = 0;
  keys.clear();
  total_keys.clear();
  while ((next_cursor = db.Scan(cursor, "GP16_KEY2_SCAN_*", 5, &keys)) != 0) {
    total_keys.insert(total_keys.end(), keys.begin(), keys.end());
    cursor = next_cursor;
  }
  ASSERT_EQ(keys.size(), 5);
  ASSERT_EQ(keys[0], "GP16_KEY2_SCAN_STRING");
  ASSERT_EQ(keys[1], "GP16_KEY2_SCAN_HASH");
  ASSERT_EQ(keys[2], "GP16_KEY2_SCAN_SET");
  ASSERT_EQ(keys[3], "GP16_KEY2_SCAN_LIST");
  ASSERT_EQ(keys[4], "GP16_KEY2_SCAN_ZSET");

  del_num = db.Del(delete_keys, &type_status);
  ASSERT_EQ(del_num, 15);
}

// Expire
TEST_F(KeysTest, ExpireTest) {
  std::string value;
  std::map<blackwidow::DataType, Status> type_status;
  int32_t ret;

  // Strings
  s = db.Set("EXPIRE_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("EXPIRE_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  s = db.SAdd("EXPIRE_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  uint64_t llen;
  s = db.RPush("EXPIRE_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  s = db.ZAdd("EXPIRE_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());

  ret = db.Expire("EXPIRE_KEY", 1, &type_status);
  ASSERT_EQ(ret, 5);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // Strings
  s = db.Get("EXPIRE_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Hashes
  s = db.HGet("EXPIRE_KEY", "EXPIRE_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Sets
  s = db.SCard("EXPIRE_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());

  // Lists
  s = db.LLen("EXPIRE_KEY", &llen);
  ASSERT_TRUE(s.IsNotFound());

  // ZSets
  s = db.ZCard("EXPIRE_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());
}

// Del
TEST_F(KeysTest, DelTest) {
  int32_t ret;
  std::string value;
  std::map<blackwidow::DataType, Status> type_status;
  std::vector<std::string> keys {"DEL_KEY"};

  // Strings
  s = db.Set("DEL_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("DEL_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  s = db.SAdd("DEL_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  uint64_t llen;
  s = db.RPush("DEL_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  // ZSets
  s = db.ZAdd("DEL_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());

  ret = db.Del(keys, &type_status);
  ASSERT_EQ(ret, 5);

  // Strings
  s = db.Get("DEL_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Hashes
  s = db.HGet("DEL_KEY", "DEL_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Sets
  s = db.SCard("DEL_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());

  // Lists
  s = db.LLen("DEL_KEY", &llen);
  ASSERT_TRUE(s.IsNotFound());

  // ZSets
  s = db.ZCard("DEL_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());
}

// Exists
TEST_F(KeysTest, ExistsTest) {
  int32_t ret;
  uint64_t llen;
  std::map<blackwidow::DataType, Status> type_status;
  std::vector<std::string> keys {"EXISTS_KEY"};

  // Strings
  s = db.Set("EXISTS_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("EXISTS_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  s = db.SAdd("EXISTS_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  s = db.RPush("EXISTS_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  // ZSets
  s = db.ZAdd("EXISTS_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());

  ret = db.Exists(keys, &type_status);
  ASSERT_EQ(ret, 5);
}

// Expireat
TEST_F(KeysTest, ExpireatTest) {
  // If the key does not exist
  std::map<blackwidow::DataType, Status> type_status;
  int32_t ret = db.Expireat("EXPIREAT_KEY", 0, &type_status);
  ASSERT_EQ(ret, 0);

  // Strings
  std::string value;
  s = db.Set("EXPIREAT_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("EXPIREAT_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  s = db.SAdd("EXPIREAT_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // List
  uint64_t llen;
  s = db.RPush("EXPIREAT_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  // ZSets
  s = db.ZAdd("EXPIREAT_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());

  int64_t unix_time;
  rocksdb::Env::Default()->GetCurrentTime(&unix_time);
  int32_t timestamp = static_cast<int32_t>(unix_time) + 1;
  ret = db.Expireat("EXPIREAT_KEY", timestamp, &type_status);
  ASSERT_EQ(ret, 5);

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  // Strings
  s = db.Get("EXPIREAT_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Hashes
  s = db.HGet("EXPIREAT_KEY", "EXPIREAT_FIELD", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Sets
  s = db.SCard("EXPIREAT_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());

  // List
  s = db.LLen("EXPIREAT_KEY", &llen);
  ASSERT_TRUE(s.IsNotFound());

  // ZSets
  s = db.ZCard("EXPIREAT_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());
}

// Persist
TEST_F(KeysTest, PersistTest) {
  // If the key does not exist
  std::map<blackwidow::DataType, Status> type_status;
  int32_t ret = db.Persist("EXPIREAT_KEY", &type_status);
  ASSERT_EQ(ret, 0);

  // If the key does not have an associated timeout
  // Strings
  std::string value;
  s = db.Set("PERSIST_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("PERSIST_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  s = db.SAdd("PERSIST_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  uint64_t llen;
  s = db.LPush("PERSIST_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  // ZSets
  s = db.ZAdd("PERSIST_KEY", {{1, "MEMBER"}}, &ret);
  ASSERT_TRUE(s.ok());

  ret = db.Persist("PERSIST_KEY", &type_status);
  ASSERT_EQ(ret, 0);

  // If the timeout was set
  ret = db.Expire("PERSIST_KEY", 1000, &type_status);
  ASSERT_EQ(ret, 5);
  ret = db.Persist("PERSIST_KEY", &type_status);
  ASSERT_EQ(ret, 5);

  std::map<blackwidow::DataType, int64_t> ttl_ret;
  ttl_ret = db.TTL("PERSIST_KEY", &type_status);
  ASSERT_EQ(ttl_ret.size(), 5);
  for (auto it = ttl_ret.begin(); it != ttl_ret.end(); it++) {
    ASSERT_EQ(it->second, -1);
  }
}

// TTL
TEST_F(KeysTest, TTLTest) {
  // If the key does not exist
  std::map<blackwidow::DataType, Status> type_status;
  std::map<blackwidow::DataType, int64_t> ttl_ret;
  ttl_ret = db.TTL("TTL_KEY", &type_status);
  ASSERT_EQ(ttl_ret.size(), 5);
  for (auto it = ttl_ret.begin(); it != ttl_ret.end(); it++) {
    ASSERT_EQ(it->second, -2);
  }

  // If the key does not have an associated timeout
  // Strings
  std::string value;
  int32_t ret = 0;
  s = db.Set("TTL_KEY", "VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("TTL_KEY", "FIELD", "VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  s = db.SAdd("TTL_KEY", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  uint64_t llen;
  s = db.RPush("TTL_KEY", {"NODE"}, &llen);
  ASSERT_TRUE(s.ok());

  // ZSets
  s = db.ZAdd("TTL_KEY", {{1, "SCORE"}}, &ret);
  ASSERT_TRUE(s.ok());

  ttl_ret = db.TTL("TTL_KEY", &type_status);
  ASSERT_EQ(ttl_ret.size(), 5);
  for (auto it = ttl_ret.begin(); it != ttl_ret.end(); it++) {
    ASSERT_EQ(it->second, -1);
  }

  // If the timeout was set
  ret = db.Expire("TTL_KEY", 10, &type_status);
  ASSERT_EQ(ret, 5);
  ttl_ret = db.TTL("TTL_KEY", &type_status);
  ASSERT_EQ(ttl_ret.size(), 5);
  for (auto it = ttl_ret.begin(); it != ttl_ret.end(); it++) {
    ASSERT_GT(it->second, 0);
    ASSERT_LE(it->second, 10);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

