//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <thread>
#include <iostream>

#include "blackwidow/blackwidow.h"

using namespace blackwidow;

class SetesTest : public ::testing::Test {
  public:
    SetesTest() {
      std::string path = "./db";
      if (access(path.c_str(), F_OK)) {
        mkdir(path.c_str(), 0755);
      }
      options.create_if_missing = true;
      s = db.Open(options, path);
    }
    virtual ~SetesTest() { }

    static void SetUpTestCase() { }
    static void TearDownTestCase() { }

    blackwidow::Options options;
    blackwidow::BlackWidow db;
    blackwidow::Status s;
};

// SAdd
TEST_F(SetesTest, SAddTest) {
  int32_t ret = 0;
  std::vector<std::string> members1 {"MM1", "MM2", "MM3", "MM2"};
  s = db.SAdd("SADD_KEY", members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SCard("SADD_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> members2 {"MM4", "MM5"};
  s = db.SAdd("SADD_KEY", members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SCard("SADD_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);

  std::map<BlackWidow::DataType, rocksdb::Status> type_status;
  db.Expire("SADD_KEY", 1, &type_status);
  ASSERT_TRUE(type_status[BlackWidow::DataType::kSetes].ok());

  // The key has timeout
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.SCard("SADD_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  std::vector<std::string> members3 {"MM7", "MM8"};
  s = db.SAdd("SADD_KEY", members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SCard("SADD_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  // Delete the key
  std::vector<rocksdb::Slice> del_keys = {"SADD_KEY"};
  type_status.clear();
  db.Del(del_keys, &type_status);
  ASSERT_TRUE(type_status[BlackWidow::DataType::kSetes].ok());
  s = db.SCard("SADD_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  std::vector<std::string> members4 {"MM9", "MM10", "MM11"};
  s = db.SAdd("SADD_KEY", members4, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SCard("SADD_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
}

// SCard
TEST_F(SetesTest, SCardTest) {
  int32_t ret = 0;
  std::vector<std::string> members {"MM1", "MM2", "MM3"};
  s = db.SAdd("SCARD_KEY", members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SCard("SCARD_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
