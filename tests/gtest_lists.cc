//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <thread>
#include <iostream>

#include "blackwidow/blackwidow.h"

using namespace blackwidow;

class ListsTest : public ::testing::Test {
 public:
  ListsTest() {
    std::string path = "./db/lists";
    if (access(path.c_str(), F_OK)) {
      mkdir(path.c_str(), 0755);
    }
    options.create_if_missing = true;
    s = db.Open(options, path);
  }
  virtual ~ListsTest() { }

  static void SetUpTestCase() { }
  static void TearDownTestCase() { }

  blackwidow::Options options;
  blackwidow::BlackWidow db;
  blackwidow::Status s;
};

// LRange
TEST_F(ListsTest, LRangeTest) {
  int32_t num;
  std::vector<std::string> values;
  for (int32_t i = 0; i < 100; i++) {
    values.push_back("LRANGE_VALUE"+ std::to_string(i));
  }
  s = db.RPush("LRANGE_KEY", values, &num);
  ASSERT_EQ(num, values.size());
  ASSERT_TRUE(s.ok());

  std::vector<std::string> result;
  s = db.LRange("LRANGE_KEY", 0, 100, &result);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(result.size(), 100);
  for (int32_t i = 0; i < 100; i++) {
    ASSERT_STREQ(result[i].c_str(), values[i].c_str());
  }
  result.clear();

  // The offsetsis negative numbers
  s = db.LRange("LRANGE_KEY", -100, 100, &result);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(result.size(), 100);
  for (int32_t i = 0; i < 100; i++) {
    ASSERT_STREQ(result[i].c_str(), values[i].c_str());
  }
  result.clear();
  
  // TODO(shq)
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
