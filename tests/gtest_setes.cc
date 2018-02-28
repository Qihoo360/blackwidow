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

// SDiff
TEST_F(SetesTest, SDiffTest) {
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFF key1 key2 key3  = {b, d}
  std::vector<std::string> gp1_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp1_members2 {"c"};
  std::vector<std::string> gp1_members3 {"a", "c", "e"};
  s = db.SAdd("GP1_SDIFF_KEY1", gp1_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SDIFF_KEY2", gp1_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP1_SDIFF_KEY3", gp1_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp1_keys {"GP1_SDIFF_KEY1",
      "GP1_SDIFF_KEY2", "GP1_SDIFF_KEY3"};
  std::vector<std::string> gp1_members_out;
  s = db.SDiff(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_members_out.size(), 2);
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "b") != gp1_members_out.end());
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "d") != gp1_members_out.end());

  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}       (expire)
  // SDIFF key1 key2 key3  = {a, b, d}
  std::map<BlackWidow::DataType, rocksdb::Status> gp1_type_status;
  db.Expire("GP1_SDIFF_KEY3", 1, &gp1_type_status);
  ASSERT_TRUE(gp1_type_status[BlackWidow::DataType::kSetes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  gp1_members_out.clear();
  s = db.SDiff(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_members_out.size(), 3);
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "a") != gp1_members_out.end());
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "b") != gp1_members_out.end());
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "d") != gp1_members_out.end());

  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}       (expire key)
  // key4 = {}              (not exist key)
  // SDIFF key1 key2 key3 key4 = {a, b, d}
  gp1_keys.push_back("GP1_SDIFF_KEY4");
  gp1_members_out.clear();
  s = db.SDiff(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_members_out.size(), 3);
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "a") != gp1_members_out.end());
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "b") != gp1_members_out.end());
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "d") != gp1_members_out.end());


  // ***************** Group 2 Test *****************
  // key1 = {}
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFF key1 key2 key3  = {}
  std::vector<std::string> gp2_members1 {};
  std::vector<std::string> gp2_members2 {"c"};
  std::vector<std::string> gp2_members3 {"a", "c", "e"};
  s = db.SAdd("GP2_SDIFF_KEY1", gp2_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.SAdd("GP2_SDIFF_KEY2", gp2_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP2_SDIFF_KEY3", gp2_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp2_keys {"GP2_SDIFF_KEY1",
      "GP2_SDIFF_KEY2", "GP2_SDIFF_KEY3"};
  std::vector<std::string> gp2_members_out;
  s = db.SDiff(gp2_keys, &gp2_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_members_out.size(), 0);


  // ***************** Group 3 Test *****************
  // key1 = {a, b, c, d}
  // SDIFF key1 = {a, b, c, d}
  std::vector<std::string> gp3_members1 {"a", "b", "c", "d"};
  s = db.SAdd("GP3_SDIFF_KEY1", gp3_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<std::string> gp3_keys {"GP3_SDIFF_KEY1"};
  std::vector<std::string> gp3_members_out;
  s = db.SDiff(gp3_keys, &gp3_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp3_members_out.size(), 4);
  ASSERT_TRUE(find(gp3_members_out.begin(),
              gp3_members_out.end(), "a") != gp3_members_out.end());
  ASSERT_TRUE(find(gp3_members_out.begin(),
              gp3_members_out.end(), "b") != gp3_members_out.end());
  ASSERT_TRUE(find(gp3_members_out.begin(),
              gp3_members_out.end(), "c") != gp3_members_out.end());
  ASSERT_TRUE(find(gp3_members_out.begin(),
              gp3_members_out.end(), "d") != gp3_members_out.end());


  // ***************** Group 4 Test *****************
  // key1 = {a, b, c, d}    (expire key);
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFF key1 key2 key3  = {}
  std::vector<std::string> gp4_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp4_members2 {"c"};
  std::vector<std::string> gp4_members3 {"a", "c", "e"};
  s = db.SAdd("GP4_SDIFF_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP4_SDIFF_KEY2", gp4_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP4_SDIFF_KEY3", gp4_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::map<BlackWidow::DataType, rocksdb::Status> gp4_type_status;
  db.Expire("GP4_SDIFF_KEY1", 1, &gp4_type_status);
  ASSERT_TRUE(gp4_type_status[BlackWidow::DataType::kSetes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  std::vector<std::string> gp4_keys {"GP4_SDIFF_KEY1",
      "GP4_SDIFF_KEY2", "GP4_SDIFF_KEY3"};
  std::vector<std::string> gp4_members_out;
  s = db.SDiff(gp4_keys, &gp4_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp4_members_out.size(), 0);


  // ***************** Group 5 Test *****************
  // key1 = {a, b, c, d}   (key1 is empty key)
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFF key1 key2 key3  = {b, d}
  std::vector<std::string> gp5_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp5_members2 {"c"};
  std::vector<std::string> gp5_members3 {"a", "c", "e"};
  s = db.SAdd("", gp5_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP5_SDIFF_KEY2", gp5_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP5_SDIFF_KEY3", gp5_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);


  std::vector<std::string> gp5_keys {"", "GP5_SDIFF_KEY2", "GP5_SDIFF_KEY3"};
  std::vector<std::string> gp5_members_out;
  s = db.SDiff(gp5_keys, &gp5_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp5_members_out.size(), 2);
  ASSERT_TRUE(find(gp5_members_out.begin(),
              gp5_members_out.end(), "b") != gp5_members_out.end());
  ASSERT_TRUE(find(gp5_members_out.begin(),
              gp5_members_out.end(), "d") != gp5_members_out.end());

  // double "GP5_SDIFF_KEY3"
  gp5_keys.push_back("GP5_SDIFF_KEY3");
  gp5_members_out.clear();
  s = db.SDiff(gp5_keys, &gp5_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp5_members_out.size(), 2);
  ASSERT_TRUE(find(gp5_members_out.begin(),
              gp5_members_out.end(), "b") != gp5_members_out.end());
  ASSERT_TRUE(find(gp5_members_out.begin(),
              gp5_members_out.end(), "d") != gp5_members_out.end());


  // ***************** Group 6 Test *****************
  // empty keys
  std::vector<std::string> gp6_keys;
  std::vector<std::string> gp6_members_out;
  s = db.SDiff(gp6_keys, &gp6_members_out);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_EQ(gp6_members_out.size(), 0);

}

// SDiffstore
TEST_F(SetesTest, SDiffstoreTest) {
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  // destination = {};
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFFSTORE destination key1 key2 key3
  // destination = {b, d}
  std::vector<std::string> gp1_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp1_members2 {"c"};
  std::vector<std::string> gp1_members3 {"a", "c", "e"};
  s = db.SAdd("GP1_SDIFFSTORE_KEY1", gp1_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SDIFFSTORE_KEY2", gp1_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP1_SDIFFSTORE_KEY3", gp1_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp1_members_out;
  std::vector<std::string> gp1_keys {"GP1_SDIFFSTORE_KEY1",
      "GP1_SDIFFSTORE_KEY2", "GP1_SDIFFSTORE_KEY3"};
  s = db.SDiffstore("GP1_SDIFFSTORE_DESTINATION1", gp1_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SMembers("GP1_SDIFFSTORE_DESTINATION1", &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_members_out.size(), 2);
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "b") != gp1_members_out.end());
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "d") != gp1_members_out.end());
  s = db.SCard("GP1_SDIFFSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  // destination = {};
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}       (expire)
  // SDIFFSTORE destination key1 key2 key3
  // destination = {a, b, d}
  std::map<BlackWidow::DataType, rocksdb::Status> gp1_type_status;
  db.Expire("GP1_SDIFFSTORE_KEY3", 1, &gp1_type_status);
  ASSERT_TRUE(gp1_type_status[BlackWidow::DataType::kSetes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  gp1_members_out.clear();
  s = db.SDiffstore("GP1_SDIFFSTORE_DESTINATION2", gp1_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SMembers("GP1_SDIFFSTORE_DESTINATION2", &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_members_out.size(), 3);
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "a") != gp1_members_out.end());
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "b") != gp1_members_out.end());
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "d") != gp1_members_out.end());
  s = db.SCard("GP1_SDIFFSTORE_DESTINATION2", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  // destination = {};
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}       (expire key)
  // key4 = {}              (not exist key)
  // SDIFFSTORE destination key1 key2 key3
  // destination = {a, b, d}
  gp1_keys.push_back("GP1_SDIFFSTORE_KEY4");
  gp1_members_out.clear();
  s = db.SDiffstore("GP1_SDIFFSTORE_DESTINATION3", gp1_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SMembers("GP1_SDIFFSTORE_DESTINATION3", &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_members_out.size(), 3);
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "a") != gp1_members_out.end());
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "b") != gp1_members_out.end());
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "d") != gp1_members_out.end());
  s = db.SCard("GP1_SDIFFSTORE_DESTINATION3", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);


  // ***************** Group 2 Test *****************
  // destination = {};
  // key1 = {}
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFFSTORE destination key1 key2 key3
  // destination = {}
  std::vector<std::string> gp2_members1 {};
  std::vector<std::string> gp2_members2 {"c"};
  std::vector<std::string> gp2_members3 {"a", "c", "e"};
  s = db.SAdd("GP2_SDIFFSTORE_KEY1", gp2_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.SAdd("GP2_SDIFFSTORE_KEY2", gp2_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP2_SDIFFSTORE_KEY3", gp2_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp2_keys {"GP2_SDIFFSTORE_KEY1",
      "GP2_SDIFFSTORE_KEY2", "GP2_SDIFFSTORE_KEY3"};
  std::vector<std::string> gp2_members_out;
  s = db.SDiffstore("GP2_SDIFFSTORE_DESTINATION1", gp2_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.SMembers("GP2_SDIFFSTORE_DESTINATION1", &gp2_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_members_out.size(), 0);
  s = db.SCard("GP2_SDIFFSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);


  // ***************** Group 3 Test *****************
  // destination = {};
  // key1 = {a, b, c, d}
  // SDIFFSTORE destination key1
  // destination = {a, b, c, d}
  std::vector<std::string> gp3_members1 {"a", "b", "c", "d"};
  s = db.SAdd("GP3_SDIFFSTORE_KEY1", gp3_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<std::string> gp3_keys {"GP3_SDIFFSTORE_KEY1"};
  std::vector<std::string> gp3_members_out;
  s = db.SDiffstore("GP3_SDIFFSTORE_DESTINATION1", gp3_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SMembers("GP3_SDIFFSTORE_DESTINATION1", &gp3_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp3_members_out.size(), 4);
  ASSERT_TRUE(find(gp3_members_out.begin(),
              gp3_members_out.end(), "a") != gp3_members_out.end());
  ASSERT_TRUE(find(gp3_members_out.begin(),
              gp3_members_out.end(), "b") != gp3_members_out.end());
  ASSERT_TRUE(find(gp3_members_out.begin(),
              gp3_members_out.end(), "c") != gp3_members_out.end());
  ASSERT_TRUE(find(gp3_members_out.begin(),
              gp3_members_out.end(), "d") != gp3_members_out.end());
  s = db.SCard("GP3_SDIFFSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);


  // ***************** Group 4 Test *****************
  // destination = {};
  // key1 = {a, b, c, d}    (expire key);
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFFSTORE destination key1 key2 key3
  // destination = {}
  std::vector<std::string> gp4_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp4_members2 {"c"};
  std::vector<std::string> gp4_members3 {"a", "c", "e"};
  s = db.SAdd("GP4_SDIFFSTORE_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP4_SDIFFSTORE_KEY2", gp4_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP4_SDIFFSTORE_KEY3", gp4_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::map<BlackWidow::DataType, rocksdb::Status> gp4_type_status;
  db.Expire("GP4_SDIFFSTORE_KEY1", 1, &gp4_type_status);
  ASSERT_TRUE(gp4_type_status[BlackWidow::DataType::kSetes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  std::vector<std::string> gp4_keys {"GP4_SDIFFSTORE_KEY1",
      "GP4_SDIFFSTORE_KEY2", "GP4_SDIFFSTORE_KEY3"};
  std::vector<std::string> gp4_members_out;
  s = db.SDiffstore("GP4_SDIFFSTORE_DESTINATION1", gp4_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.SMembers("GP4_SDIFFSTORE_DESTINATION1", &gp4_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp4_members_out.size(), 0);
  s = db.SCard("GP4_SDIFFSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);


  // ***************** Group 5 Test *****************
  // the destination already exists, it is overwritten
  // destination = {a, x, l}
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFFSTORE destination key1 key2 key3
  // destination = {b, d}
  std::vector<std::string> gp5_destination_members {"a", "x", "l"};
  std::vector<std::string> gp5_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp5_members2 {"c"};
  std::vector<std::string> gp5_members3 {"a", "c", "e"};
  s = db.SAdd("GP5_SDIFFSTORE_DESTINATION1", gp5_destination_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SAdd("GP5_SDIFFSTORE_KEY1", gp5_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP5_SDIFFSTORE_KEY2", gp5_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP5_SDIFFSTORE_KEY3", gp5_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp5_members_out;
  std::vector<std::string> gp5_keys {"GP5_SDIFFSTORE_KEY1",
      "GP5_SDIFFSTORE_KEY2", "GP5_SDIFFSTORE_KEY3"};
  s = db.SDiffstore("GP5_SDIFFSTORE_DESTINATION1", gp5_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SMembers("GP5_SDIFFSTORE_DESTINATION1", &gp5_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp5_members_out.size(), 2);
  ASSERT_TRUE(find(gp5_members_out.begin(),
              gp5_members_out.end(), "b") != gp5_members_out.end());
  ASSERT_TRUE(find(gp5_members_out.begin(),
              gp5_members_out.end(), "d") != gp5_members_out.end());
  s = db.SCard("GP5_SDIFFSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);


  // ***************** Group 6 Test *****************
  // test destination equal key1 (the destination already exists, it is
  // overwritten)
  // destination = {a, b, c, d};
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFFSTORE destination destination key2 key3
  // destination = {b, d}
  std::vector<std::string> gp6_destination_members {"a", "b", "c", "d"};
  std::vector<std::string> gp6_members2 {"c"};
  std::vector<std::string> gp6_members3 {"a", "c", "e"};
  s = db.SAdd("GP6_SDIFFSTORE_DESTINATION1", gp6_destination_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP6_SDIFFSTORE_KEY2", gp6_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP6_SDIFFSTORE_KEY3", gp6_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp6_members_out;
  std::vector<std::string> gp6_keys {"GP6_SDIFFSTORE_DESTINATION1",
      "GP6_SDIFFSTORE_KEY2", "GP6_SDIFFSTORE_KEY3"};
  s = db.SDiffstore("GP6_SDIFFSTORE_DESTINATION1", gp6_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SMembers("GP6_SDIFFSTORE_DESTINATION1", &gp6_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp6_members_out.size(), 2);
  ASSERT_TRUE(find(gp6_members_out.begin(),
              gp6_members_out.end(), "b") != gp6_members_out.end());
  ASSERT_TRUE(find(gp6_members_out.begin(),
              gp6_members_out.end(), "d") != gp6_members_out.end());
  s = db.SCard("GP6_SDIFFSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);


  // ***************** Group 7 Test *****************
  // test destination exist but timeout (the destination already exists, it is
  // overwritten)
  // destination = {a, x, l};
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}
  // SDIFFSTORE destination key1 key2 key3
  // destination = {b, d}
  std::vector<std::string> gp7_destination_members {"a", "x", "l"};
  std::vector<std::string> gp7_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp7_members2 {"c"};
  std::vector<std::string> gp7_members3 {"a", "c", "e"};
  s = db.SAdd("GP7_SDIFFSTORE_DESTINATION1", gp7_destination_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SAdd("GP7_SDIFFSTORE_KEY1", gp7_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP7_SDIFFSTORE_KEY2", gp7_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP7_SDIFFSTORE_KEY3", gp7_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::map<BlackWidow::DataType, rocksdb::Status> gp7_type_status;
  db.Expire("GP7_SDIFFSTORE_DESTINATION1", 1, &gp7_type_status);
  ASSERT_TRUE(gp4_type_status[BlackWidow::DataType::kSetes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  std::vector<std::string> gp7_members_out;
  std::vector<std::string> gp7_keys {"GP7_SDIFFSTORE_KEY1",
      "GP7_SDIFFSTORE_KEY2", "GP7_SDIFFSTORE_KEY3"};
  s = db.SDiffstore("GP7_SDIFFSTORE_DESTINATION1", gp7_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SMembers("GP7_SDIFFSTORE_DESTINATION1", &gp7_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp7_members_out.size(), 2);
  ASSERT_TRUE(find(gp7_members_out.begin(),
              gp7_members_out.end(), "b") != gp7_members_out.end());
  ASSERT_TRUE(find(gp7_members_out.begin(),
              gp7_members_out.end(), "d") != gp7_members_out.end());
  s = db.SCard("GP7_SDIFFSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
}

// SInter
TEST_F(SetesTest, SInterTest) {
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTER key1 key2 key3  = {a, c}
  std::vector<std::string> gp1_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp1_members2 {"a", "c"};
  std::vector<std::string> gp1_members3 {"a", "c", "e"};
  s = db.SAdd("GP1_SINTER_KEY1", gp1_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SINTER_KEY2", gp1_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP1_SINTER_KEY3", gp1_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp1_keys {"GP1_SINTER_KEY1",
      "GP1_SINTER_KEY2", "GP1_SINTER_KEY3"};
  std::vector<std::string> gp1_members_out;
  s = db.SInter(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_members_out.size(), 2);
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "a") != gp1_members_out.end());
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "c") != gp1_members_out.end());

  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}       (expire)
  // SINTER key1 key2 key3  = {}
  std::map<BlackWidow::DataType, rocksdb::Status> gp1_type_status;
  db.Expire("GP1_SINTER_KEY3", 1, &gp1_type_status);
  ASSERT_TRUE(gp1_type_status[BlackWidow::DataType::kSetes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  gp1_members_out.clear();
  s = db.SInter(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_members_out.size(), 0);


  // ***************** Group 2 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}
  // SINTER key1 key2 key3 not_exist_key = {}
  std::vector<std::string> gp2_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp2_members2 {"c"};
  std::vector<std::string> gp2_members3 {"a", "c", "e"};
  s = db.SAdd("GP2_SINTER_KEY1", gp2_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP2_SINTER_KEY2", gp2_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);
  s = db.SAdd("GP2_SINTER_KEY3", gp2_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp2_keys {"GP2_SINTER_KEY1",
      "GP2_SINTER_KEY2", "GP2_SINTER_KEY3", "NOT_EXIST_KEY"};
  std::vector<std::string> gp2_members_out;
  s = db.SInter(gp2_keys, &gp2_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_members_out.size(), 0);


  // ***************** Group 3 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {}
  // SINTER key1 key2 key3 = {}
  std::vector<std::string> gp3_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp3_members2 {"a", "c"};
  std::vector<std::string> gp3_members3 {"a", "b", "c"};
  s = db.SAdd("GP3_SINTER_KEY1", gp3_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP3_SINTER_KEY2", gp3_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP3_SINTER_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.SRem("GP3_SINTER_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SCard("GP3_SINTER_KEY3", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  std::vector<std::string> gp3_members_out;
  s = db.SMembers("GP3_SINTER_KEY3", &gp3_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp3_members_out.size(), 0);

  gp3_members_out.clear();
  std::vector<std::string> gp3_keys {"GP3_SINTER_KEY1",
      "GP3_SINTER_KEY2", "GP3_SINTER_KEY3"};
  s = db.SInter(gp3_keys, &gp3_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp3_members_out.size(), 0);


  // ***************** Group 4 Test *****************
  // key1 = {}
  // key2 = {a, c}
  // key3 = {a, b, c, d}
  // SINTER key1 key2 key3 = {}
  std::vector<std::string> gp4_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp4_members2 {"a", "c"};
  std::vector<std::string> gp4_members3 {"a", "b", "c", "d"};
  s = db.SAdd("GP4_SINTER_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP4_SINTER_KEY2", gp4_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP4_SINTER_KEY3", gp4_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  s = db.SRem("GP4_SINTER_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SCard("GP4_SINTER_KEY1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  std::vector<std::string> gp4_members_out;
  s = db.SMembers("GP4_SINTER_KEY1", &gp4_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp4_members_out.size(), 0);

  gp4_members_out.clear();
  std::vector<std::string> gp4_keys {"GP4_SINTER_KEY1",
      "GP4_SINTER_KEY2", "GP4_SINTER_KEY3"};
  s = db.SInter(gp4_keys, &gp4_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp4_members_out.size(), 0);


  // ***************** Group 5 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, b, c}
  // SINTER key1 key2 key2 key3 = {a, c}
  std::vector<std::string> gp5_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp5_members2 {"a", "c"};
  std::vector<std::string> gp5_members3 {"a", "b", "c"};
  s = db.SAdd("GP5_SINTER_KEY1", gp5_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP5_SINTER_KEY2", gp5_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP5_SINTER_KEY3", gp5_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp5_members_out;
  std::vector<std::string> gp5_keys {"GP5_SINTER_KEY1",
      "GP5_SINTER_KEY2", "GP5_SINTER_KEY2", "GP5_SINTER_KEY3"};
  s = db.SInter(gp5_keys, &gp5_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp5_members_out.size(), 2);
  gp5_members_out.clear();
  s = db.SMembers("GP5_SINTER_KEY1", &gp5_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(find(gp5_members_out.begin(),
              gp5_members_out.end(), "a") != gp5_members_out.end());
  ASSERT_TRUE(find(gp5_members_out.begin(),
              gp5_members_out.end(), "c") != gp5_members_out.end());
}

// SInterstore
TEST_F(SetesTest, SInterstoreTest) {
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTERSTORE destination key1 key2 key3
  // destination = {a, c}
  std::vector<std::string> gp1_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp1_members2 {"a", "c"};
  std::vector<std::string> gp1_members3 {"a", "c", "e"};
  s = db.SAdd("GP1_SINTERSTORE_KEY1", gp1_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SINTERSTORE_KEY2", gp1_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP1_SINTERSTORE_KEY3", gp1_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp1_keys {"GP1_SINTERSTORE_KEY1",
      "GP1_SINTERSTORE_KEY2", "GP1_SINTERSTORE_KEY3"};
  std::vector<std::string> gp1_members_out;
  s = db.SInterstore("GP1_SINTERSTORE_DESTINATION1", gp1_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SMembers("GP1_SINTERSTORE_DESTINATION1", &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_members_out.size(), 2);
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "a") != gp1_members_out.end());
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "c") != gp1_members_out.end());
  s = db.SCard("GP1_SDIFFSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);


  // ***************** Group 2 Test *****************
  // the destination already exists, it is overwritten.
  // destination = {a, x, l}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTERSTORE destination key1 key2 key3
  // destination = {a, c}
  std::vector<std::string> gp2_destination_members {"a", "x", "l"};
  std::vector<std::string> gp2_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp2_members2 {"a", "c"};
  std::vector<std::string> gp2_members3 {"a", "c", "e"};
  s = db.SAdd("GP2_SINTERSTORE_DESTINATION1", gp2_destination_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SAdd("GP2_SINTERSTORE_KEY1", gp2_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP2_SINTERSTORE_KEY2", gp2_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP2_SINTERSTORE_KEY3", gp2_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp2_keys {"GP2_SINTERSTORE_KEY1",
      "GP2_SINTERSTORE_KEY2", "GP2_SINTERSTORE_KEY3"};
  std::vector<std::string> gp2_members_out;
  s = db.SInterstore("GP2_SINTERSTORE_DESTINATION1", gp2_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SMembers("GP2_SINTERSTORE_DESTINATION1", &gp2_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_members_out.size(), 2);
  ASSERT_TRUE(find(gp2_members_out.begin(),
              gp2_members_out.end(), "a") != gp2_members_out.end());
  ASSERT_TRUE(find(gp2_members_out.begin(),
              gp2_members_out.end(), "c") != gp2_members_out.end());
  s = db.SCard("GP2_SINTERSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);


  // ***************** Group 3 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTERSTORE destination key1 key2 key3 not_exist_key
  // destination = {}
  std::vector<std::string> gp3_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp3_members2 {"a", "c"};
  std::vector<std::string> gp3_members3 {"a", "c", "e"};
  s = db.SAdd("GP3_SINTERSTORE_KEY1", gp3_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP3_SINTERSTORE_KEY2", gp3_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP3_SINTERSTORE_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp3_keys {"GP3_SINTERSTORE_KEY1",
      "GP3_SINTERSTORE_KEY2", "GP3_SINTERSTORE_KEY3",
      "GP3_SINTERSTORE_NOT_EXIST_KEY"};
  std::vector<std::string> gp3_members_out;
  s = db.SInterstore("GP3_SINTERSTORE_DESTINATION1", gp3_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.SMembers("GP3_SINTERSTORE_DESTINATION1", &gp3_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp3_members_out.size(), 0);
  s = db.SCard("GP3_SINTERSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);


  // ***************** Group 4 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}          (expire key);
  // key3 = {a, c, e}
  // SINTERSTORE destination key1 key2 key3
  // destination = {}
  std::vector<std::string> gp4_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp4_members2 {"a", "c"};
  std::vector<std::string> gp4_members3 {"a", "c", "e"};
  s = db.SAdd("GP4_SINTERSTORE_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP4_SINTERSTORE_KEY2", gp4_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP4_SINTERSTORE_KEY3", gp4_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::map<BlackWidow::DataType, rocksdb::Status> gp4_type_status;
  db.Expire("GP4_SINTERSTORE_KEY2", 1, &gp4_type_status);
  ASSERT_TRUE(gp4_type_status[BlackWidow::DataType::kSetes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  std::vector<std::string> gp4_keys {"GP4_SINTERSTORE_KEY1",
      "GP4_SINTERSTORE_KEY2", "GP4_SINTERSTORE_KEY3"};
  std::vector<std::string> gp4_members_out;
  s = db.SInterstore("GP4_SINTERSTORE_DESTINATION1", gp4_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.SMembers("GP4_SINTERSTORE_DESTINATION1", &gp4_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp4_members_out.size(), 0);
  s = db.SCard("GP4_SINTERSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);


  // ***************** Group 5 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}    (expire key);
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTERSTORE destination key1 key2 key3
  // destination = {}
  std::vector<std::string> gp5_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp5_members2 {"a", "c"};
  std::vector<std::string> gp5_members3 {"a", "c", "e"};
  s = db.SAdd("GP5_SINTERSTORE_KEY1", gp5_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP5_SINTERSTORE_KEY2", gp5_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP5_SINTERSTORE_KEY3", gp5_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::map<BlackWidow::DataType, rocksdb::Status> gp5_type_status;
  db.Expire("GP5_SINTERSTORE_KEY1", 1, &gp5_type_status);
  ASSERT_TRUE(gp5_type_status[BlackWidow::DataType::kSetes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  std::vector<std::string> gp5_keys {"GP5_SINTERSTORE_KEY1",
      "GP5_SINTERSTORE_KEY2", "GP5_SINTERSTORE_KEY3"};
  std::vector<std::string> gp5_members_out;
  s = db.SInterstore("GP5_SINTERSTORE_DESTINATION1", gp5_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.SMembers("GP5_SINTERSTORE_DESTINATION1", &gp5_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp5_members_out.size(), 0);
  s = db.SCard("GP5_SINTERSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);


  // ***************** Group 6 Test *****************
  // destination = {}
  // key1 = {}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTERSTORE destination key1 key2 key3
  // destination = {}
  std::vector<std::string> gp6_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp6_members2 {"a", "c"};
  std::vector<std::string> gp6_members3 {"a", "c", "e"};
  s = db.SAdd("GP6_SINTERSTORE_KEY1", gp6_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP6_SINTERSTORE_KEY2", gp6_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP6_SINTERSTORE_KEY3", gp6_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.SRem("GP6_SINTERSTORE_KEY1", gp6_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SCard("GP6_SINTERSTORE_KEY1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  std::vector<std::string> gp6_keys {"GP6_SINTERSTORE_KEY1",
      "GP6_SINTERSTORE_KEY2", "GP6_SINTERSTORE_KEY3"};
  std::vector<std::string> gp6_members_out;
  s = db.SInterstore("GP6_SINTERSTORE_DESTINATION1", gp6_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.SMembers("GP6_SINTERSTORE_DESTINATION1", &gp6_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp4_members_out.size(), 0);
  s = db.SCard("GP6_SINTERSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);


  // ***************** Group 7 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SINTERSTORE destination not_exist_key key1 key2 key3
  // destination = {}
  std::vector<std::string> gp7_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp7_members2 {"a", "c"};
  std::vector<std::string> gp7_members3 {"a", "c", "e"};
  s = db.SAdd("GP7_SINTERSTORE_KEY1", gp7_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP7_SINTERSTORE_KEY2", gp7_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP7_SINTERSTORE_KEY3", gp7_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp7_keys {"GP7_SINTERSTORE_NOT_EXIST_KEY",
      "GP7_SINTERSTORE_KEY1", "GP7_SINTERSTORE_KEY2", "GP7_SINTERSTORE_KEY3"};
  std::vector<std::string> gp7_members_out;
  s = db.SInterstore("GP7_SINTERSTORE_DESTINATION1", gp7_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  s = db.SMembers("GP7_SINTERSTORE_DESTINATION1", &gp7_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp7_members_out.size(), 0);
  s = db.SCard("GP7_SINTERSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);


  // ***************** Group 8 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, b, c, d}
  // key3 = {a, b, c, d}
  // SINTERSTORE destination key1 key2 key3
  // destination = {a, b, c, d}
  std::vector<std::string> gp8_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp8_members2 {"a", "b", "c", "d"};
  std::vector<std::string> gp8_members3 {"a", "b", "c", "d"};
  s = db.SAdd("GP8_SINTERSTORE_KEY1", gp8_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP8_SINTERSTORE_KEY2", gp8_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP8_SINTERSTORE_KEY3", gp8_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<std::string> gp8_keys {"GP8_SINTERSTORE_KEY1",
      "GP8_SINTERSTORE_KEY2", "GP8_SINTERSTORE_KEY3", };
  std::vector<std::string> gp8_members_out;
  s = db.SInterstore("GP8_SINTERSTORE_DESTINATION1", gp8_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SMembers("GP8_SINTERSTORE_DESTINATION1", &gp8_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp8_members_out.size(), 4);
  ASSERT_TRUE(find(gp8_members_out.begin(),
              gp8_members_out.end(), "a") != gp8_members_out.end());
  ASSERT_TRUE(find(gp8_members_out.begin(),
              gp8_members_out.end(), "b") != gp8_members_out.end());
  ASSERT_TRUE(find(gp8_members_out.begin(),
              gp8_members_out.end(), "c") != gp8_members_out.end());
  ASSERT_TRUE(find(gp8_members_out.begin(),
              gp8_members_out.end(), "d") != gp8_members_out.end());
  s = db.SCard("GP8_SINTERSTORE_DESTINATION1", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
}


// SIsmember
TEST_F(SetesTest, SIsmemberTest) {
  int32_t ret = 0;
  std::vector<std::string> members {"MEMBER"};
  s = db.SAdd("SISMEMBER_KEY", members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  // Not exist set key
  s = db.SIsmember("SISMEMBER_NOT_EXIST_KEY", "MEMBER", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  // Not exist set member
  s = db.SIsmember("SISMEMBER_KEY", "NOT_EXIST_MEMBER", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  s = db.SIsmember("SISMEMBER_KEY", "MEMBER", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  // Expire set key
  std::map<BlackWidow::DataType, rocksdb::Status> type_status;
  db.Expire("SISMEMBER_KEY", 1, &type_status);
  ASSERT_TRUE(type_status[BlackWidow::DataType::kSetes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.SIsmember("SISMEMBER_KEY", "MEMBER", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);
}

// SMembers
TEST_F(SetesTest, SMembersTest) {
  int32_t ret = 0;
  std::vector<std::string> mid_members_in;
  mid_members_in.push_back("MID_MEMBER1");
  mid_members_in.push_back("MID_MEMBER2");
  mid_members_in.push_back("MID_MEMBER3");
  s = db.SAdd("B_SMEMBERS_KEY", mid_members_in, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> members_out;
  s = db.SMembers("B_SMEMBERS_KEY", &members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(members_out.size(), 3);
  ASSERT_TRUE(find(members_out.begin(),
              members_out.end(), "MID_MEMBER1") != members_out.end());
  ASSERT_TRUE(find(members_out.begin(),
              members_out.end(), "MID_MEMBER2") != members_out.end());
  ASSERT_TRUE(find(members_out.begin(),
              members_out.end(), "MID_MEMBER3") != members_out.end());

  // Insert some kv who's position above "mid kv"
  std::vector<std::string> pre_members_in;
  pre_members_in.push_back("PRE_MEMBER1");
  pre_members_in.push_back("PRE_MEMBER2");
  pre_members_in.push_back("PRE_MEMBER3");
  s = db.SAdd("A_SMEMBERS_KEY", pre_members_in, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  members_out.clear();
  s = db.SMembers("B_SMEMBERS_KEY", &members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(members_out.size(), 3);
  ASSERT_TRUE(find(members_out.begin(),
              members_out.end(), "MID_MEMBER1") != members_out.end());
  ASSERT_TRUE(find(members_out.begin(),
              members_out.end(), "MID_MEMBER2") != members_out.end());
  ASSERT_TRUE(find(members_out.begin(),
              members_out.end(), "MID_MEMBER3") != members_out.end());

  // Insert some kv who's position below "mid kv"
  std::vector<std::string> suf_members_in;
  suf_members_in.push_back("SUF_MEMBER1");
  suf_members_in.push_back("SUF_MEMBER2");
  suf_members_in.push_back("SUF_MEMBER3");
  s = db.SAdd("C_SMEMBERS_KEY", suf_members_in, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  members_out.clear();
  s = db.SMembers("B_SMEMBERS_KEY", &members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(members_out.size(), 3);
  ASSERT_TRUE(find(members_out.begin(),
              members_out.end(), "MID_MEMBER1") != members_out.end());
  ASSERT_TRUE(find(members_out.begin(),
              members_out.end(), "MID_MEMBER2") != members_out.end());
  ASSERT_TRUE(find(members_out.begin(),
              members_out.end(), "MID_MEMBER3") != members_out.end());

  // SMembers timeout setes
  members_out.clear();
  std::map<BlackWidow::DataType, rocksdb::Status> type_status;
  db.Expire("B_SMEMBERS_KEY", 1, &type_status);
  ASSERT_TRUE(type_status[BlackWidow::DataType::kSetes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.SMembers("B_SMEMBERS_KEY", &members_out);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(members_out.size(), 0);

  // SMembers not exist setes
  members_out.clear();
  s = db.SMembers("SMEMBERS_NOT_EXIST_KEY", &members_out);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(members_out.size(), 0);
}

// SRem
TEST_F(SetesTest, SRemTest) {
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  std::vector<std::string> gp1_members {"a", "b", "c", "d"};
  s = db.SAdd("GP1_SREM_KEY", gp1_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<std::string> gp1_del_members {"a", "b"};
  s = db.SRem("GP1_SREM_KEY", gp1_del_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  std::vector<std::string> gp1_members_out;
  s = db.SMembers("GP1_SREM_KEY", &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_members_out.size(), 2);
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "c") != gp1_members_out.end());
  ASSERT_TRUE(find(gp1_members_out.begin(),
              gp1_members_out.end(), "d") != gp1_members_out.end());

  s = db.SCard("GP1_SREM_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);


  // ***************** Group 2 Test *****************
  // srem not exist members
  std::vector<std::string> gp2_members {"a", "b", "c", "d"};
  s = db.SAdd("GP2_SREM_KEY", gp2_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<std::string> gp2_del_members {"e", "f"};
  s = db.SRem("GP2_SREM_KEY", gp2_del_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  std::vector<std::string> gp2_members_out;
  s = db.SMembers("GP2_SREM_KEY", &gp2_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_members_out.size(), 4);
  ASSERT_TRUE(find(gp2_members_out.begin(),
              gp2_members_out.end(), "a") != gp2_members_out.end());
  ASSERT_TRUE(find(gp2_members_out.begin(),
              gp2_members_out.end(), "b") != gp2_members_out.end());
  ASSERT_TRUE(find(gp2_members_out.begin(),
              gp2_members_out.end(), "c") != gp2_members_out.end());
  ASSERT_TRUE(find(gp2_members_out.begin(),
              gp2_members_out.end(), "d") != gp2_members_out.end());

  s = db.SCard("GP2_SREM_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);


  // ***************** Group 3 Test *****************
  // srem not exist key
  std::vector<std::string> gp3_del_members{"a", "b", "c"};
  s = db.SRem("GP3_NOT_EXIST_KEY", gp3_del_members, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);


  // ***************** Group 4 Test *****************
  // srem timeout key
  std::vector<std::string> gp4_members {"a", "b", "c", "d"};
  s = db.SAdd("GP4_SREM_KEY", gp4_members, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::map<BlackWidow::DataType, rocksdb::Status> gp4_type_status;
  db.Expire("GP4_SREM_KEY", 1, &gp4_type_status);
  ASSERT_TRUE(gp4_type_status[BlackWidow::DataType::kSetes].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  std::vector<std::string> gp4_del_members {"a", "b"};
  s = db.SRem("GP4_SREM_KEY", gp4_del_members, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  std::vector<std::string> gp4_members_out;
  s = db.SMembers("GP4_SREM_KEY", &gp4_members_out);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(gp4_members_out.size(), 0);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
