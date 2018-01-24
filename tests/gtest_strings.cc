//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <thread>
#include <iostream>

#include "blackwidow/blackwidow.h"

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
  s = db.Set("KEY", "HELLO WORLD");
  ASSERT_TRUE(s.ok());
  s = db.Setrange("KEY", 6, "REDIS", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 11);
  s = db.Get("KEY", &value);
  ASSERT_STREQ(value.c_str(), "HELLO REDIS");

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

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

