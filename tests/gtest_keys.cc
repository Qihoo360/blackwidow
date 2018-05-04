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
  kvs.push_back({"SCAN_KEY1", "SCAN_VALUE1"});
  kvs.push_back({"SCAN_KEY2", "SCAN_VALUE2"});
  kvs.push_back({"SCAN_KEY3", "SCAN_VALUE3"});
  kvs.push_back({"SCAN_KEY4", "SCAN_VALUE4"});
  kvs.push_back({"SCAN_KEY5", "SCAN_VALUE5"});

  // Note: Noise data is used to test the priority between 'match' and 'count'
  kvs.push_back({"NSCAN_KEY1", "SCAN_VALUE1"});
  kvs.push_back({"NSCAN_KEY2", "SCAN_VALUE2"});
  kvs.push_back({"NSCAN_KEY3", "SCAN_VALUE3"});
  kvs.push_back({"NSCAN_KEY4", "SCAN_VALUE4"});
  kvs.push_back({"NSCAN_KEY5", "SCAN_VALUE5"});
  s = db.MSet(kvs);
  ASSERT_TRUE(s.ok());

  // Hashes
  std::vector<blackwidow::BlackWidow::FieldValue> fvs1;
  fvs1.push_back({"TEST_FIELD1", "TEST_VALUE1"});
  fvs1.push_back({"TEST_FIELD2", "TEST_VALUE2"});
  fvs1.push_back({"TEST_FIELD3", "TEST_VALUE3"});
  fvs1.push_back({"TEST_FIELD4", "TEST_VALUE4"});
  fvs1.push_back({"TEST_FIELD5", "TEST_VALUE5"});
  s = db.HMSet("SCAN_KEY6", fvs1);
  s = db.HMSet("SCAN_KEY7", fvs1);
  s = db.HMSet("SCAN_KEY8", fvs1);
  s = db.HMSet("SCAN_KEY9", fvs1);
  s = db.HMSet("SCAN_KEY10", fvs1);
  ASSERT_TRUE(s.ok());

  // Sets
  int32_t ret;
  std::vector<std::string> members;
  members.push_back("MEMBER1");
  members.push_back("MEMBER2");
  members.push_back("MEMBER3");
  members.push_back("MEMBER4");
  members.push_back("MEMBER5");
  s = db.SAdd("SCAN_KEY11", members, &ret);
  s = db.SAdd("SCAN_KEY12", members, &ret);
  s = db.SAdd("SCAN_KEY13", members, &ret);
  s = db.SAdd("SCAN_KEY14", members, &ret);
  s = db.SAdd("SCAN_KEY15", members, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  uint64_t list_len;
  std::vector<std::string> nodes;
  nodes.push_back("NODE1");
  nodes.push_back("NODE2");
  nodes.push_back("NODE3");
  nodes.push_back("NODE4");
  nodes.push_back("NODE5");
  s = db.RPush("SCAN_KEY16", nodes, &list_len);
  s = db.RPush("SCAN_KEY17", nodes, &list_len);
  s = db.RPush("SCAN_KEY18", nodes, &list_len);
  s = db.RPush("SCAN_KEY19", nodes, &list_len);
  s = db.RPush("SCAN_KEY20", nodes, &list_len);
  ASSERT_TRUE(s.ok());

  // TODO(wxj) other data types

  // Iterate by data types and check data type
  std::vector<std::string> keys;
  cursor_ret = db.Scan(0, "SCAN*", 10, &keys);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(keys.size(), 5);
  ASSERT_STREQ(keys[0].c_str(), "SCAN_KEY1");
  ASSERT_STREQ(keys[1].c_str(), "SCAN_KEY2");
  ASSERT_STREQ(keys[2].c_str(), "SCAN_KEY3");
  ASSERT_STREQ(keys[3].c_str(), "SCAN_KEY4");
  ASSERT_STREQ(keys[4].c_str(), "SCAN_KEY5");

  keys.clear();
  ASSERT_EQ(cursor_ret, 10);
  cursor_ret = db.Scan(cursor_ret, "SCAN*", 5, &keys);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(keys.size(), 5);
  ASSERT_STREQ(keys[0].c_str(), "SCAN_KEY10");
  ASSERT_STREQ(keys[1].c_str(), "SCAN_KEY6");
  ASSERT_STREQ(keys[2].c_str(), "SCAN_KEY7");
  ASSERT_STREQ(keys[3].c_str(), "SCAN_KEY8");
  ASSERT_STREQ(keys[4].c_str(), "SCAN_KEY9");

  keys.clear();
  ASSERT_EQ(cursor_ret, 15);
  cursor_ret = db.Scan(cursor_ret, "SCAN*", 5, &keys);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(keys.size(), 5);
  ASSERT_STREQ(keys[0].c_str(), "SCAN_KEY11");
  ASSERT_STREQ(keys[1].c_str(), "SCAN_KEY12");
  ASSERT_STREQ(keys[2].c_str(), "SCAN_KEY13");
  ASSERT_STREQ(keys[3].c_str(), "SCAN_KEY14");
  ASSERT_STREQ(keys[4].c_str(), "SCAN_KEY15");

  keys.clear();
  ASSERT_EQ(cursor_ret, 20);
  cursor_ret = db.Scan(cursor_ret, "SCAN*", 5, &keys);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(keys.size(), 5);
  ASSERT_STREQ(keys[0].c_str(), "SCAN_KEY16");
  ASSERT_STREQ(keys[1].c_str(), "SCAN_KEY17");
  ASSERT_STREQ(keys[2].c_str(), "SCAN_KEY18");
  ASSERT_STREQ(keys[3].c_str(), "SCAN_KEY19");
  ASSERT_STREQ(keys[4].c_str(), "SCAN_KEY20");

  keys.clear();
  cursor_ret = db.Scan(cursor_ret, "SCAN*", 5, &keys);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(keys.size(), 0);
  ASSERT_EQ(cursor_ret, 0);

  // Iterate over data types
  int64_t cursor_origin;
  for (; ;) {
    keys.clear();
    cursor_origin = cursor_ret;
    cursor_ret = db.Scan(cursor_ret, "SCAN*", 3, &keys);
    if (cursor_ret != 0) {
      ASSERT_EQ(cursor_ret, cursor_origin + 3);
    } else {
      break;
    }
  }

  // Repeat scan the same parameters should return the same result
  for (int32_t i = 0; i < 10; i++) {
    keys.clear();
    cursor_ret = db.Scan(3, "SCAN*", 7, &keys);
    ASSERT_EQ(keys.size(), 5);
    ASSERT_EQ(cursor_ret, 10);
  }

  // If the key already expired
  std::map<BlackWidow::DataType, Status> type_status;
  ret = db.Expire("SCAN_KEY1", 1, &type_status);
  ASSERT_GE(ret, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  keys.clear();
  cursor_ret = db.Scan(0, "SCAN*", 10, &keys);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(keys.size(), 5);
  ASSERT_STREQ(keys[0].c_str(), "SCAN_KEY2");
  ASSERT_STREQ(keys[1].c_str(), "SCAN_KEY3");
  ASSERT_STREQ(keys[2].c_str(), "SCAN_KEY4");
  ASSERT_STREQ(keys[3].c_str(), "SCAN_KEY5");
  ASSERT_STREQ(keys[4].c_str(), "SCAN_KEY10");
}

// Expire
TEST_F(KeysTest, ExpireTest) {
  std::string value;
  std::map<BlackWidow::DataType, Status> type_status;
  int32_t ret;

  // Strings
  s = db.Set("EXPIRE_KEY", "EXPIRE_VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("EXPIRE_KEY", "EXPIRE_FIELD", "EXPIRE_VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  std::vector<std::string> members {"MEMBERS"};
  s = db.SAdd("EXPIRE_KEY", members, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  uint64_t llen;
  std::vector<std::string> nodes {"NODE"};
  s = db.RPush("EXPIRE_KEY", nodes, &llen);
  ASSERT_TRUE(s.ok());

  ret = db.Expire("EXPIRE_KEY", 1, &type_status);
  ASSERT_EQ(ret, 4);

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

  // TODO(wxj) other types
}

// Del
TEST_F(KeysTest, DelTest) {
  int32_t ret;
  std::string value;
  std::map<BlackWidow::DataType, Status> type_status;
  std::vector<std::string> keys {"DEL_KEY"};

  // Strings
  s = db.Set("DEL_KEY", "DEL_VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("DEL_KEY", "DEL_FIELD", "DEL_VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  std::vector<std::string> members {"MEMBERS"};
  s = db.SAdd("DEL_KEY", members, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  uint64_t llen;
  std::vector<std::string> nodes {"NODE"};
  s = db.RPush("DEL_KEY", nodes, &llen);
  ASSERT_TRUE(s.ok());

  ret = db.Del(keys, &type_status);
  ASSERT_EQ(ret, 4);

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

  // TODO(wxj) other types
}

// Exists
TEST_F(KeysTest, ExistsTest) {
  int32_t ret;
  uint64_t llen;
  std::map<BlackWidow::DataType, Status> type_status;
  std::vector<std::string> keys {"EXISTS_KEY"};

  // Strings
  s = db.Set("EXISTS_KEY", "EXISTS_VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("EXISTS_KEY", "EXISTS_FIELD", "EXISTS_VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  std::vector<std::string> members {"MEMBERS"};
  s = db.SAdd("EXISTS_KEY", members, &ret);
  ASSERT_TRUE(s.ok());

  // Lists
  std::vector<std::string> nodes {"NODE"};
  s = db.RPush("EXISTS_KEY", nodes, &llen);
  ASSERT_TRUE(s.ok());

  ret = db.Exists(keys, &type_status);
  ASSERT_EQ(ret, 4);

  // TODO(wxj) other types
}

// Expireat
TEST_F(KeysTest, ExpireatTest) {
  // If the key does not exist
  std::map<BlackWidow::DataType, Status> type_status;
  int32_t ret = db.Expireat("EXPIREAT_KEY", 0, &type_status);
  ASSERT_EQ(ret, 0);

  // Strings
  std::string value;
  s = db.Set("EXPIREAT_KEY", "EXPIREAT_VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("EXPIREAT_KEY", "EXPIREAT_FIELD", "EXPIREAT_VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  std::vector<std::string> members {"MEMBERS"};
  s = db.SAdd("EXPIREAT_KEY", members, &ret);
  ASSERT_TRUE(s.ok());

  // TODO(shq) other types

  int64_t unix_time;
  rocksdb::Env::Default()->GetCurrentTime(&unix_time);
  int32_t timestamp = static_cast<int32_t>(unix_time) + 1;
  ret = db.Expireat("EXPIREAT_KEY", timestamp, &type_status);
  ASSERT_EQ(ret, 3);

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
  s = db.Set("PERSIST_KEY", "PERSIST_VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("PERSIST_KEY", "PERSIST_FIELD", "PERSIST_VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  std::vector<std::string> members {"MM1", "MM2", "MM3", "MM2"};
  s = db.SAdd("PERSIST_KEY", members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  // Lists
  uint64_t llen;
  std::vector<std::string> nodes {"NODE"};
  s = db.LPush("PERSIST_KEY", nodes, &llen);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(llen, 1);

  // TODO(wxj) other types
  ret = db.Persist("PERSIST_KEY", &type_status);
  ASSERT_EQ(ret, 0);

  // If the timeout was set
  ret = db.Expire("PERSIST_KEY", 1000, &type_status);
  ASSERT_EQ(ret, 4);
  ret = db.Persist("PERSIST_KEY", &type_status);
  ASSERT_EQ(ret, 4);

  std::map<BlackWidow::DataType, int64_t> ttl_ret;
  ttl_ret = db.TTL("PERSIST_KEY", &type_status);
  ASSERT_EQ(ttl_ret.size(), 4);
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
  ASSERT_EQ(ttl_ret.size(), 4);
  for (auto it = ttl_ret.begin(); it != ttl_ret.end(); it++) {
    ASSERT_EQ(it->second, -2);
  }

  // If the key does not have an associated timeout
  // Strings
  std::string value;
  int32_t ret = 0;
  s = db.Set("TTL_KEY", "TTL_VALUE");
  ASSERT_TRUE(s.ok());

  // Hashes
  s = db.HSet("TTL_KEY", "TTL_FIELD", "TTL_VALUE", &ret);
  ASSERT_TRUE(s.ok());

  // Sets
  std::vector<std::string> members {"MM1", "MM2", "MM3", "MM2"};
  s = db.SAdd("TTL_KEY", members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  // Lists
  uint64_t llen;
  std::vector<std::string> nodes {"NODE"};
  s = db.RPush("TTL_KEY", nodes, &llen);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(llen, 1);

  // TODO(wxj) other types
  ttl_ret = db.TTL("TTL_KEY", &type_status);
  ASSERT_EQ(ttl_ret.size(), 4);
  for (auto it = ttl_ret.begin(); it != ttl_ret.end(); it++) {
    ASSERT_EQ(it->second, -1);
  }

  // If the timeout was set
  ret = db.Expire("TTL_KEY", 10, &type_status);
  ASSERT_EQ(ret, 4);
  ttl_ret = db.TTL("TTL_KEY", &type_status);
  ASSERT_EQ(ttl_ret.size(), 4);
  for (auto it = ttl_ret.begin(); it != ttl_ret.end(); it++) {
    ASSERT_GT(it->second, 0);
    ASSERT_LE(it->second, 10);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

