//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <thread>
#include <iostream>

#include "blackwidow/blackwidow.h"

using namespace blackwidow;

class StringsTest : public ::testing::Test {
 public:
  StringsTest() {
    options.create_if_missing = true;
    s = db.Open(options, "./db");
  }
  virtual ~StringsTest() { }

  static void SetUpTestCase() { }
  static void TearDownTestCase() { }

  blackwidow::Options options;
  blackwidow::BlackWidow db;
  blackwidow::Status s;
};

// Set
TEST_F(StringsTest, SetTest) {
  s = db.Set("TEST_KEY", "TEST_VALUE");
  ASSERT_TRUE(s.ok());
}

// Get
TEST_F(StringsTest, GetTest) {
  std::string value;
  s = db.Get("TEST_KEY", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_STREQ(value.c_str(), "TEST_VALUE");
}

// MSet
TEST_F(StringsTest, MSetTest) {
  std::vector<BlackWidow::KeyValue> kvs;
  kvs.push_back({"", "MSET_EMPTY_VALUE"});
  kvs.push_back({"MSET_TEST_KEY1", "MSET_TEST_VALUE1"});
  kvs.push_back({"MSET_TEST_KEY2", "MSET_TEST_VALUE2"});
  kvs.push_back({"MSET_TEST_KEY3", "MSET_TEST_VALUE3"});
  kvs.push_back({"MSET_TEST_KEY3", "MSET_TEST_VALUE3"});
  s = db.MSet(kvs);
  ASSERT_TRUE(s.ok());
}

// MGet
TEST_F(StringsTest, MGetTest) {
  std::vector<std::string> values;
  std::vector<std::string> keys {"", "MSET_TEST_KEY1",
    "MSET_TEST_KEY2", "MSET_TEST_KEY3", "MSET_TEST_KEY_NOT_EXIST"};
  s = db.MGet(keys, &values);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(values.size(), 5);
  ASSERT_EQ(values[0], "MSET_EMPTY_VALUE");
  ASSERT_EQ(values[1], "MSET_TEST_VALUE1");
  ASSERT_EQ(values[2], "MSET_TEST_VALUE2");
  ASSERT_EQ(values[3], "MSET_TEST_VALUE3");
  ASSERT_EQ(values[4], "");
}

// Setnx
TEST_F(StringsTest, SetnxTest) {
  // If the key was not set, return 0
  int32_t ret;
  s = db.Setnx("TEST_KEY", "TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  // If the key was set, return 1
  s = db.Setnx("SETNX_KEY", "TEST_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
}

// Setrange
TEST_F(StringsTest, SetrangeTest) {
  std::string value;
  int32_t ret;
  s = db.Set("SETRANGE_KEY", "HELLO WORLD");
  ASSERT_TRUE(s.ok());
  s = db.Setrange("SETRANGE_KEY", 6, "REDIS", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 11);
  s = db.Get("SETRANGE_KEY", &value);
  ASSERT_STREQ(value.c_str(), "HELLO REDIS");

  std::vector<std::string> keys {"SETRANGE_KEY"};
  std::map<BlackWidow::DataType, Status> type_status;
  ret = db.Del(keys, &type_status);
  ASSERT_EQ(ret, 1);
  // If not exist, padded with zero-bytes to make offset fit
  s = db.Setrange("SETRANGE_KEY", 6, "REDIS", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 11);
  s = db.Get("SETRANGE_KEY", &value);
  ASSERT_STREQ(value.c_str(), "\x00\x00\x00\x00\x00\x00REDIS");

  // If the offset less than 0
  s = db.Setrange("SETRANGE_KEY", -1, "REDIS", &ret);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Beyond the maximum offset(2^29-1)
  s = db.Setrange("SETRANGE_KEY", 536870912, "REDIS", &ret);
  ASSERT_TRUE(s.IsInvalidArgument());
}

// Append
TEST_F(StringsTest, AppendTest) {
  int32_t ret;
  std::string value;
  s = db.Append("APPEND_KEY", "HELLO", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);

  s = db.Append("APPEND_KEY", " WORLD", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 11);

  s = db.Get("APPEND_KEY", &value);
  ASSERT_STREQ(value.c_str(), "HELLO WORLD");
}

// BitCount
TEST_F(StringsTest, BitCountTest) {
  int32_t ret;
  s = db.Set("BITCOUNT_KEY", "foobar");
  ASSERT_TRUE(s.ok());

  // Not have offset
  s = db.BitCount("BITCOUNT_KEY", 0, -1, &ret, false);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 26);

  // Have offset
  s = db.BitCount("BITCOUNT_KEY", 0, 0, &ret, true);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.BitCount("BITCOUNT_KEY", 1, 1, &ret, true);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 6);
}

// Decrby
TEST_F(StringsTest, DecrbyTest) {
  int64_t ret;
  // If the key is not exist
  s = db.Decrby("DECRBY_KEY", 5, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, -5);

  // If the key contains a string that can not be represented as integer
  s = db.Set("DECRBY_KEY", "DECRBY_VALUE");
  ASSERT_TRUE(s.ok());
  s = db.Decrby("DECRBY_KEY", 5, &ret);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Less than the minimum number -9223372036854775808
  s = db.Decrby("DECRBY_KEY", 9223372036854775807, &ret);
  ASSERT_TRUE(s.IsInvalidArgument());
}

// Setex
TEST_F(StringsTest, SetexTest) {
  std::string value;
  s = db.Setex("SETEX_KEY", "SETEX_VALUE", 1);
  ASSERT_TRUE(s.ok());

  // The key is not timeout
  s = db.Get("SETEX_KEY", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, "SETEX_VALUE");

  // The key is timeout
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.Get("SETEX_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());

  // If the ttl equal 0
  s = db.Setex("SETEX_KEY", "SETEX_VALUE", 0);
  ASSERT_TRUE(s.IsInvalidArgument());

  // The ttl is negative
  s = db.Setex("SETEX_KEY", "SETEX_VALUE", -1);
  ASSERT_TRUE(s.IsInvalidArgument());
}

// Strlen
TEST_F(StringsTest, StrlenTest) {
  int32_t strlen;
  // The value is empty
  s = db.Set("STRLEN_EMPTY_KEY", "");
  ASSERT_TRUE(s.ok());
  s = db.Strlen("STRLEN_EMPTY_KEY", &strlen);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(strlen, 0);

  // The key is not exist
  s = db.Strlen("STRLEN_NOT_EXIST_KEY", &strlen);
  ASSERT_EQ(strlen, 0);

  s = db.Set("STRLEN_KEY", "STRLEN_VALUE");
  ASSERT_TRUE(s.ok());
  s = db.Strlen("STRLEN_KEY", &strlen);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(strlen, 12);
}

// Expire
TEST_F(StringsTest, ExpireTest) {
  std::string value;
  std::map<BlackWidow::DataType, Status> type_status;
  int32_t ret;
  s = db.Set("EXPIRE_KEY", "EXPIREVALUE");
  ASSERT_TRUE(s.ok());
  ret = db.Expire("EXPIRE_KEY", 1, &type_status);
  for (auto it = type_status.begin(); it != type_status.end(); it++) {
    if (it->first == BlackWidow::DataType::kStrings) {
      ASSERT_TRUE(it->second.ok());
    } else {
      ASSERT_TRUE(it->second.IsNotFound());
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  s = db.Get("EXPIRE_KEY", &value);
  ASSERT_TRUE(s.IsNotFound());
}

// Del
TEST_F(StringsTest, DelTest) {
  int32_t ret;
  std::map<BlackWidow::DataType, Status> type_status;
  std::vector<std::string> keys {"DEL_KEY"};
  s = db.Set("DEL_KEY", "EXPIREVALUE");
  ASSERT_TRUE(s.ok());
  s = db.HSet("DEL_KEY", "DEL_FIELD", "DEL_VALUE", &ret);
  ASSERT_TRUE(s.ok());
  ret = db.Del(keys, &type_status);
  for (auto it = type_status.begin(); it != type_status.end(); it++) {
    if (it->first == BlackWidow::DataType::kStrings) {
      ASSERT_TRUE(it->second.ok());
    } else if (it->first == BlackWidow::DataType::kHashes) {
      ASSERT_TRUE(it->second.ok());
    } else {
      ASSERT_TRUE(it->second.IsNotFound());
    }
  }
  ASSERT_EQ(ret, 1);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

