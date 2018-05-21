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
    s = db.Open(options, path);
  }
  virtual ~KeysTest() { }

  static void SetUpTestCase() { }
  static void TearDownTestCase() { }

  blackwidow::Options options;
  blackwidow::BlackWidow db;
  blackwidow::Status s;
};

// Scan
// Note: This test needs to execute at first because all of the data is
// predetermined.
TEST_F(KeysTest, ScanTest) {
  // Keys
  std::vector<BlackWidow::KeyValue> kvs;
  int64_t cursor_ret;
  kvs.push_back({"SCAN_KEY_A", "VALUE"});
  kvs.push_back({"SCAN_KEY_B", "VALUE"});
  kvs.push_back({"SCAN_KEY_C", "VALUE"});
  kvs.push_back({"SCAN_KEY_D", "VALUE"});
  kvs.push_back({"SCAN_KEY_E", "VALUE"});

  // Note: Noise data is used to test the priority between 'match' and 'count'
  kvs.push_back({"NSCAN_KEY_A", "VALUE"});
  kvs.push_back({"NSCAN_KEY_B", "VALUE"});
  kvs.push_back({"NSCAN_KEY_C", "VALUE"});
  kvs.push_back({"NSCAN_KEY_D", "VALUE"});
  kvs.push_back({"NSCAN_KEY_E", "VALUE"});
  s = db.MSet(kvs);
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HMSet("SCAN_KEY_F", {{"FIELD", "VALUE"}});
  s = db.HMSet("SCAN_KEY_G", {{"FIELD", "VALUE"}});
  s = db.HMSet("SCAN_KEY_H", {{"FIELD", "VALUE"}});
  s = db.HMSet("SCAN_KEY_I", {{"FIELD", "VALUE"}});
  s = db.HMSet("SCAN_KEY_J", {{"FIELD", "VALUE"}});
  ASSERT_TRUE(s.ok());

  // Sets
  int32_t ret;
  s = db.SAdd("SCAN_KEY_K", {"MEMBER"}, &ret);
  s = db.SAdd("SCAN_KEY_L", {"MEMBER"}, &ret);
  s = db.SAdd("SCAN_KEY_M", {"MEMBER"}, &ret);
  s = db.SAdd("SCAN_KEY_N", {"MEMBER"}, &ret);
  s = db.SAdd("SCAN_KEY_O", {"MEMBER"}, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  uint64_t list_len;
  s = db.RPush("SCAN_KEY_P", {"NODE"}, &list_len);
  s = db.RPush("SCAN_KEY_Q", {"NODE"}, &list_len);
  s = db.RPush("SCAN_KEY_R", {"NODE"}, &list_len);
  s = db.RPush("SCAN_KEY_S", {"NODE"}, &list_len);
  s = db.RPush("SCAN_KEY_T", {"NODE"}, &list_len);
  ASSERT_TRUE(s.ok());

  // ZSets
  s = db.ZAdd("SCAN_KEY_U", {{1,"MEMBER"}}, &ret);
  s = db.ZAdd("SCAN_KEY_V", {{1,"MEMBER"}}, &ret);
  s = db.ZAdd("SCAN_KEY_W", {{1,"MEMBER"}}, &ret);
  s = db.ZAdd("SCAN_KEY_X", {{1,"MEMBER"}}, &ret);
  s = db.ZAdd("SCAN_KEY_Y", {{1,"MEMBER"}}, &ret);

  // Iterate by data types and check data type
  std::vector<std::string> keys;
  cursor_ret = db.Scan(0, "SCAN*", 10, &keys);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(keys.size(), 5);
  ASSERT_STREQ(keys[0].c_str(), "SCAN_KEY_A");
  ASSERT_STREQ(keys[1].c_str(), "SCAN_KEY_B");
  ASSERT_STREQ(keys[2].c_str(), "SCAN_KEY_C");
  ASSERT_STREQ(keys[3].c_str(), "SCAN_KEY_D");
  ASSERT_STREQ(keys[4].c_str(), "SCAN_KEY_E");

  keys.clear();
  ASSERT_EQ(cursor_ret, 10);
  cursor_ret = db.Scan(cursor_ret, "SCAN*", 5, &keys);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(keys.size(), 5);
  ASSERT_STREQ(keys[0].c_str(), "SCAN_KEY_F");
  ASSERT_STREQ(keys[1].c_str(), "SCAN_KEY_G");
  ASSERT_STREQ(keys[2].c_str(), "SCAN_KEY_H");
  ASSERT_STREQ(keys[3].c_str(), "SCAN_KEY_I");
  ASSERT_STREQ(keys[4].c_str(), "SCAN_KEY_J");

  keys.clear();
  ASSERT_EQ(cursor_ret, 15);
  cursor_ret = db.Scan(cursor_ret, "SCAN*", 5, &keys);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(keys.size(), 5);
  ASSERT_STREQ(keys[0].c_str(), "SCAN_KEY_K");
  ASSERT_STREQ(keys[1].c_str(), "SCAN_KEY_L");
  ASSERT_STREQ(keys[2].c_str(), "SCAN_KEY_M");
  ASSERT_STREQ(keys[3].c_str(), "SCAN_KEY_N");
  ASSERT_STREQ(keys[4].c_str(), "SCAN_KEY_O");

  keys.clear();
  ASSERT_EQ(cursor_ret, 20);
  cursor_ret = db.Scan(cursor_ret, "SCAN*", 5, &keys);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(keys.size(), 5);
  ASSERT_STREQ(keys[0].c_str(), "SCAN_KEY_P");
  ASSERT_STREQ(keys[1].c_str(), "SCAN_KEY_Q");
  ASSERT_STREQ(keys[2].c_str(), "SCAN_KEY_R");
  ASSERT_STREQ(keys[3].c_str(), "SCAN_KEY_S");
  ASSERT_STREQ(keys[4].c_str(), "SCAN_KEY_T");

  keys.clear();
  cursor_ret = db.Scan(cursor_ret, "SCAN*", 5, &keys);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(keys.size(), 5);
  ASSERT_STREQ(keys[0].c_str(), "SCAN_KEY_U");
  ASSERT_STREQ(keys[1].c_str(), "SCAN_KEY_V");
  ASSERT_STREQ(keys[2].c_str(), "SCAN_KEY_W");
  ASSERT_STREQ(keys[3].c_str(), "SCAN_KEY_X");
  ASSERT_STREQ(keys[4].c_str(), "SCAN_KEY_Y");

  keys.clear();
  cursor_ret = db.Scan(cursor_ret, "SCAN*", 5, &keys);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(keys.size(), 0);
}

// Expire
TEST_F(KeysTest, ExpireTest) {
  std::string value;
  std::map<BlackWidow::DataType, Status> type_status;
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
  std::map<BlackWidow::DataType, Status> type_status;
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
  std::map<BlackWidow::DataType, Status> type_status;
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
  std::map<BlackWidow::DataType, Status> type_status;
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
  std::map<BlackWidow::DataType, Status> type_status;
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

  std::map<BlackWidow::DataType, int64_t> ttl_ret;
  ttl_ret = db.TTL("PERSIST_KEY", &type_status);
  ASSERT_EQ(ttl_ret.size(), 5);
  for (auto it = ttl_ret.begin(); it != ttl_ret.end(); it++) {
    ASSERT_EQ(it->second, -1);
  }
}

// TTL
TEST_F(KeysTest, TTLTest) {
  // If the key does not exist
  std::map<BlackWidow::DataType, Status> type_status;
  std::map<BlackWidow::DataType, int64_t> ttl_ret;
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

