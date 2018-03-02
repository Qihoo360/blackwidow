//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <thread>
#include <iostream>

#include "blackwidow/blackwidow.h"

using namespace blackwidow;

class SetsTest : public ::testing::Test {
 public:
  SetsTest() {
    std::string path = "./db";
    if (access(path.c_str(), F_OK)) {
      mkdir(path.c_str(), 0755);
    }
    options.create_if_missing = true;
    s = db.Open(options, path);
  }
  virtual ~SetsTest() { }

  static void SetUpTestCase() { }
  static void TearDownTestCase() { }

  blackwidow::Options options;
  blackwidow::BlackWidow db;
  blackwidow::Status s;
};

static bool members_match(blackwidow::BlackWidow *const db,
                          const Slice& key,
                          const std::vector<std::string>& expect_members) {
  std::vector<std::string> mm_out;
  Status s = db->SMembers(key, &mm_out);
  if (!s.ok() && !s.IsNotFound()) {
    return false;
  }
  if (mm_out.size() != expect_members.size()) {
    return false;
  }
  if (s.IsNotFound() && expect_members.empty()) {
    return true;
  }
  for (const auto& member : expect_members) {
    if (find(mm_out.begin(), mm_out.end(), member) == mm_out.end()) {
      return false;
    }
  }
  return true;
}

static bool members_match(const std::vector<std::string>& mm_out,
                          const std::vector<std::string>& expect_members) {
  if (mm_out.size() != expect_members.size()) {
    return false;
  }
  for (const auto& member : expect_members) {
    if (find(mm_out.begin(), mm_out.end(), member) == mm_out.end()) {
      return false;
    }
  }
  return true;
}

static bool size_match(blackwidow::BlackWidow *const db,
                       const Slice& key,
                       int32_t expect_size) {
  int32_t size = 0;
  Status s = db->SCard(key, &size);
  if (!s.ok() && !s.IsNotFound()) {
    return false;
  }
  if (s.IsNotFound() && !expect_size) {
    return true;
  }
  return size == expect_size;
}

static bool make_expired(blackwidow::BlackWidow *const db,
                         const Slice& key) {
  std::map<BlackWidow::DataType, rocksdb::Status> type_status;
  int ret = db->Expire(key, 1, &type_status);
  if (!ret || !type_status[BlackWidow::DataType::kSets].ok()) {
    return false;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  return true;
}

// SAdd
TEST_F(SetsTest, SAddTest) {
  int32_t ret = 0;
  std::vector<std::string> members1 {"a", "b", "c", "b"};
  s = db.SAdd("SADD_KEY", members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "SADD_KEY", 3));
  ASSERT_TRUE(members_match(&db, "SADD_KEY", {"a", "b", "c"}));

  std::vector<std::string> members2 {"d", "e"};
  s = db.SAdd("SADD_KEY", members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "SADD_KEY", 5));
  ASSERT_TRUE(members_match(&db, "SADD_KEY", {"a", "b", "c", "d", "e"}));

  // The key has timeout
  ASSERT_TRUE(make_expired(&db, "SADD_KEY"));
  ASSERT_TRUE(size_match(&db, "SADD_KEY", 0));

  std::vector<std::string> members3 {"a", "b"};
  s = db.SAdd("SADD_KEY", members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "SADD_KEY", 2));
  ASSERT_TRUE(members_match(&db, "SADD_KEY", {"a", "b"}));

  // Delete the key
  std::vector<rocksdb::Slice> del_keys = {"SADD_KEY"};
  std::map<BlackWidow::DataType, blackwidow::Status> type_status;
  db.Del(del_keys, &type_status);
  ASSERT_TRUE(type_status[BlackWidow::DataType::kSets].ok());
  ASSERT_TRUE(size_match(&db, "SADD_KEY", 0));
  ASSERT_TRUE(members_match(&db, "SADD_KEY", {}));

  std::vector<std::string> members4 {"a", "x", "l"};
  s = db.SAdd("SADD_KEY", members4, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "SADD_KEY", 3));
  ASSERT_TRUE(members_match(&db, "SADD_KEY", {"a", "x", "l"}));
}

// SCard
TEST_F(SetsTest, SCardTest) {
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
TEST_F(SetsTest, SDiffTest) {
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
  ASSERT_TRUE(members_match(gp1_members_out, {"b", "d"}));

  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}       (expire)
  // SDIFF key1 key2 key3  = {a, b, d}
  std::map<BlackWidow::DataType, rocksdb::Status> gp1_type_status;
  db.Expire("GP1_SDIFF_KEY3", 1, &gp1_type_status);
  ASSERT_TRUE(gp1_type_status[BlackWidow::DataType::kSets].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  gp1_members_out.clear();
  s = db.SDiff(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp1_members_out, {"a", "b", "d"}));

  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}       (expire key)
  // key4 = {}              (not exist key)
  // SDIFF key1 key2 key3 key4 = {a, b, d}
  gp1_keys.push_back("GP1_SDIFF_KEY4");
  gp1_members_out.clear();
  s = db.SDiff(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp1_members_out, {"a", "b", "d"}));


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
  ASSERT_TRUE(members_match(gp2_members_out, {}));


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
  ASSERT_TRUE(members_match(gp3_members_out, {"a", "b", "c", "d"}));


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

  ASSERT_TRUE(make_expired(&db, "GP4_SDIFF_KEY1"));

  std::vector<std::string> gp4_keys {"GP4_SDIFF_KEY1",
      "GP4_SDIFF_KEY2", "GP4_SDIFF_KEY3"};
  std::vector<std::string> gp4_members_out;
  s = db.SDiff(gp4_keys, &gp4_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp4_members_out, {}));


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
  ASSERT_TRUE(members_match(gp5_members_out, {"b", "d"}));

  // double "GP5_SDIFF_KEY3"
  gp5_keys.push_back("GP5_SDIFF_KEY3");
  gp5_members_out.clear();
  s = db.SDiff(gp5_keys, &gp5_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp5_members_out, {"b", "d"}));


  // ***************** Group 6 Test *****************
  // empty keys
  std::vector<std::string> gp6_keys;
  std::vector<std::string> gp6_members_out;
  s = db.SDiff(gp6_keys, &gp6_members_out);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(members_match(gp6_members_out, {}));
}

// SDiffstore
TEST_F(SetsTest, SDiffstoreTest) {
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
  ASSERT_TRUE(size_match(&db, "GP1_SDIFFSTORE_DESTINATION1", 2));
  ASSERT_TRUE(members_match(&db, "GP1_SDIFFSTORE_DESTINATION1", {"b", "d"}));

  // destination = {};
  // key1 = {a, b, c, d}
  // key2 = {c}
  // key3 = {a, c, e}       (expire)
  // SDIFFSTORE destination key1 key2 key3
  // destination = {a, b, d}
  std::map<BlackWidow::DataType, rocksdb::Status> gp1_type_status;
  db.Expire("GP1_SDIFFSTORE_KEY3", 1, &gp1_type_status);
  ASSERT_TRUE(gp1_type_status[BlackWidow::DataType::kSets].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  gp1_members_out.clear();
  s = db.SDiffstore("GP1_SDIFFSTORE_DESTINATION2", gp1_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP1_SDIFFSTORE_DESTINATION2", 3));
  ASSERT_TRUE(members_match(&db, "GP1_SDIFFSTORE_DESTINATION2",
              {"a", "b", "d"}));

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
  ASSERT_TRUE(size_match(&db, "GP1_SDIFFSTORE_DESTINATION3", 3));
  ASSERT_TRUE(members_match(&db, "GP1_SDIFFSTORE_DESTINATION3",
              {"a", "b", "d"}));


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
  ASSERT_TRUE(size_match(&db, "GP2_SDIFFSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP2_SDIFFSTORE_DESTINATION1", {}));


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
  ASSERT_TRUE(size_match(&db, "GP3_SDIFFSTORE_DESTINATION1", 4));
  ASSERT_TRUE(members_match(&db, "GP3_SDIFFSTORE_DESTINATION1",
              {"a", "b", "c", "d"}));


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

  ASSERT_TRUE(make_expired(&db, "GP4_SDIFFSTORE_KEY1"));

  std::vector<std::string> gp4_keys {"GP4_SDIFFSTORE_KEY1",
      "GP4_SDIFFSTORE_KEY2", "GP4_SDIFFSTORE_KEY3"};
  std::vector<std::string> gp4_members_out;
  s = db.SDiffstore("GP4_SDIFFSTORE_DESTINATION1", gp4_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP4_SDIFFSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP4_SDIFFSTORE_DESTINATION1", {}));


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

  std::vector<std::string> gp5_keys {"GP5_SDIFFSTORE_KEY1",
      "GP5_SDIFFSTORE_KEY2", "GP5_SDIFFSTORE_KEY3"};
  s = db.SDiffstore("GP5_SDIFFSTORE_DESTINATION1", gp5_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "GP5_SDIFFSTORE_DESTINATION1", 2));
  ASSERT_TRUE(members_match(&db, "GP5_SDIFFSTORE_DESTINATION1", {"b", "d"}));


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

  std::vector<std::string> gp6_keys {"GP6_SDIFFSTORE_DESTINATION1",
      "GP6_SDIFFSTORE_KEY2", "GP6_SDIFFSTORE_KEY3"};
  s = db.SDiffstore("GP6_SDIFFSTORE_DESTINATION1", gp6_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "GP6_SDIFFSTORE_DESTINATION1", 2));
  ASSERT_TRUE(members_match(&db, "GP6_SDIFFSTORE_DESTINATION1", {"b", "d"}));


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

  ASSERT_TRUE(make_expired(&db, "GP7_SDIFFSTORE_DESTINATION1"));

  std::vector<std::string> gp7_keys {"GP7_SDIFFSTORE_KEY1",
      "GP7_SDIFFSTORE_KEY2", "GP7_SDIFFSTORE_KEY3"};
  s = db.SDiffstore("GP7_SDIFFSTORE_DESTINATION1", gp7_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "GP7_SDIFFSTORE_DESTINATION1", 2));
  ASSERT_TRUE(members_match(&db, "GP7_SDIFFSTORE_DESTINATION1", {"b", "d"}));
}

// SInter
TEST_F(SetsTest, SInterTest) {
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
  ASSERT_TRUE(members_match(gp1_members_out, {"a", "c"}));

  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}       (expire)
  // SINTER key1 key2 key3  = {}
  ASSERT_TRUE(make_expired(&db, "GP1_SINTER_KEY3"));

  gp1_members_out.clear();
  s = db.SInter(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp1_members_out, {}));


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
  ASSERT_TRUE(members_match(gp2_members_out, {}));


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
  ASSERT_TRUE(members_match(gp3_members_out, {}));

  gp3_members_out.clear();
  std::vector<std::string> gp3_keys {"GP3_SINTER_KEY1",
      "GP3_SINTER_KEY2", "GP3_SINTER_KEY3"};
  s = db.SInter(gp3_keys, &gp3_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp3_members_out, {}));


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
  ASSERT_TRUE(members_match(gp4_members_out, {}));


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
  ASSERT_TRUE(members_match(gp5_members_out, {"a", "c"}));
}

// SInterstore
TEST_F(SetsTest, SInterstoreTest) {
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
  s = db.SInterstore("GP1_SINTERSTORE_DESTINATION1", gp1_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "GP1_SINTERSTORE_DESTINATION1", 2));
  ASSERT_TRUE(members_match(&db, "GP1_SINTERSTORE_DESTINATION1", {"a", "c"}));


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
  s = db.SInterstore("GP2_SINTERSTORE_DESTINATION1", gp2_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  ASSERT_TRUE(size_match(&db, "GP2_SINTERSTORE_DESTINATION1", 2));
  ASSERT_TRUE(members_match(&db, "GP2_SINTERSTORE_DESTINATION1", {"a", "c"}));


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
  s = db.SInterstore("GP3_SINTERSTORE_DESTINATION1", gp3_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP3_SINTERSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP3_SINTERSTORE_DESTINATION1", {}));


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

  ASSERT_TRUE(make_expired(&db, "GP4_SINTERSTORE_KEY2"));

  std::vector<std::string> gp4_keys {"GP4_SINTERSTORE_KEY1",
      "GP4_SINTERSTORE_KEY2", "GP4_SINTERSTORE_KEY3"};
  s = db.SInterstore("GP4_SINTERSTORE_DESTINATION1", gp4_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP4_SINTERSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP4_SINTERSTORE_DESTINATION1", {}));


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

  ASSERT_TRUE(make_expired(&db, "GP5_SINTERSTORE_KEY1"));

  std::vector<std::string> gp5_keys {"GP5_SINTERSTORE_KEY1",
      "GP5_SINTERSTORE_KEY2", "GP5_SINTERSTORE_KEY3"};
  s = db.SInterstore("GP5_SINTERSTORE_DESTINATION1", gp5_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP5_SINTERSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP5_SINTERSTORE_DESTINATION1", {}));


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
  s = db.SInterstore("GP6_SINTERSTORE_DESTINATION1", gp6_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP6_SINTERSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP6_SINTERSTORE_DESTINATION1", {}));


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
  s = db.SInterstore("GP7_SINTERSTORE_DESTINATION1", gp7_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  ASSERT_TRUE(size_match(&db, "GP7_SINTERSTORE_DESTINATION1", 0));
  ASSERT_TRUE(members_match(&db, "GP7_SINTERSTORE_DESTINATION1", {}));


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
  ASSERT_TRUE(size_match(&db, "GP8_SINTERSTORE_DESTINATION1", 4));
  ASSERT_TRUE(members_match(&db, "GP8_SINTERSTORE_DESTINATION1",
              {"a", "b", "c", "d"}));
}


// SIsmember
TEST_F(SetsTest, SIsmemberTest) {
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
  ASSERT_TRUE(type_status[BlackWidow::DataType::kSets].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.SIsmember("SISMEMBER_KEY", "MEMBER", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);
}

// SMembers
TEST_F(SetsTest, SMembersTest) {
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
  ASSERT_TRUE(members_match(members_out, mid_members_in));

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
  ASSERT_TRUE(members_match(members_out, mid_members_in));

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
  ASSERT_TRUE(members_match(members_out, mid_members_in));

  // SMembers timeout setes
  ASSERT_TRUE(make_expired(&db, "B_SMEMBERS_KEY"));
  ASSERT_TRUE(members_match(&db, "B_SMEMBERS_KEY", {}));

  // SMembers not exist setes
  ASSERT_TRUE(members_match(&db, "SMEMBERS_NOT_EXIST_KEY", {}));
}

// SMove
TEST_F(SetsTest, SMoveTest) {
  int32_t ret = 0;
  // ***************** Group 1 Test *****************
  // source = {a, b, c, d}
  // destination = {a, c}
  // SMove source destination d
  // source = {a, b, c}
  // destination = {a, c, d}
  std::vector<std::string> gp1_source {"a", "b", "c", "d"};
  std::vector<std::string> gp1_destination {"a", "c"};
  s = db.SAdd("GP1_SMOVE_SOURCE", gp1_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SMOVE_DESTINATION", gp1_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.SMove("GP1_SMOVE_SOURCE", "GP1_SMOVE_DESTINATION", "d", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  ASSERT_TRUE(size_match(&db, "GP1_SMOVE_SOURCE", 3));
  ASSERT_TRUE(members_match(&db, "GP1_SMOVE_SOURCE", {"a", "b", "c"}));
  ASSERT_TRUE(size_match(&db, "GP1_SMOVE_DESTINATION", 3));
  ASSERT_TRUE(members_match(&db, "GP1_SMOVE_DESTINATION", {"a", "c", "d"}));


  // ***************** Group 2 Test *****************
  // source = {a, b, c, d}
  // destination = {a, c}   (expire key);
  // SMove source destination d
  // source = {a, b, c}
  // destination = {d}
  std::vector<std::string> gp2_source {"a", "b", "c", "d"};
  std::vector<std::string> gp2_destination {"a", "c"};
  s = db.SAdd("GP2_SMOVE_SOURCE", gp2_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP2_SMOVE_DESTINATION", gp2_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  ASSERT_TRUE(make_expired(&db, "GP2_SMOVE_DESTINATION"));

  s = db.SMove("GP2_SMOVE_SOURCE", "GP2_SMOVE_DESTINATION", "d", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  ASSERT_TRUE(size_match(&db, "GP2_SMOVE_SOURCE", 3));
  ASSERT_TRUE(members_match(&db, "GP2_SMOVE_SOURCE", {"a", "b", "c"}));
  ASSERT_TRUE(size_match(&db, "GP2_SMOVE_DESTINATION", 1));
  ASSERT_TRUE(members_match(&db, "GP2_SMOVE_DESTINATION", {"d"}));


  // ***************** Group 3 Test *****************
  // source = {a, x, l}
  // destination = {}
  // SMove source destination x
  // source = {a, l}
  // destination = {x}
  std::vector<std::string> gp3_source {"a", "x", "l"};
  std::vector<std::string> gp3_destination {"a", "b"};
  s = db.SAdd("GP3_SMOVE_SOURCE", gp3_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  s = db.SAdd("GP3_SMOVE_DESTINATION", gp3_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.SRem("GP3_SMOVE_DESTINATION", gp3_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SCard("GP3_SMOVE_DESTINATION", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.SMove("GP3_SMOVE_SOURCE", "GP3_SMOVE_DESTINATION", "x", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  ASSERT_TRUE(size_match(&db, "GP3_SMOVE_SOURCE", 2));
  ASSERT_TRUE(members_match(&db, "GP3_SMOVE_SOURCE", {"a", "l"}));
  ASSERT_TRUE(size_match(&db, "GP3_SMOVE_DESTINATION", 1));
  ASSERT_TRUE(members_match(&db, "GP3_SMOVE_DESTINATION", {"x"}));


  // ***************** Group 4 Test *****************
  // source = {a, x, l}
  // SMove source not_exist_key x
  // source = {a, l}
  // not_exist_key = {x}
  std::vector<std::string> gp4_source {"a", "x", "l"};
  s = db.SAdd("GP4_SMOVE_SOURCE", gp4_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.SMove("GP4_SMOVE_SOURCE", "GP4_SMOVE_NOT_EXIST_KEY", "x", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  ASSERT_TRUE(size_match(&db, "GP4_SMOVE_SOURCE", 2));
  ASSERT_TRUE(members_match(&db, "GP4_SMOVE_SOURCE", {"a", "l"}));
  ASSERT_TRUE(size_match(&db, "GP4_SMOVE_NOT_EXIST_KEY", 1));
  ASSERT_TRUE(members_match(&db, "GP4_SMOVE_NOT_EXIST_KEY", {"x"}));


  // ***************** Group 5 Test *****************
  // source = {}
  // destination = {a, x, l}
  // SMove source destination x
  // source = {}
  // destination = {a, x, l}
  std::vector<std::string> gp5_source {"a", "b"};
  std::vector<std::string> gp5_destination {"a", "x", "l"};
  s = db.SAdd("GP5_SMOVE_SOURCE", gp5_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP5_SMOVE_DESTINATION", gp5_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.SRem("GP5_SMOVE_SOURCE", gp5_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SCard("GP5_SMOVE_SOURCE", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.SMove("GP5_SMOVE_SOURCE", "GP5_SMOVE_DESTINATION", "x", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  ASSERT_TRUE(size_match(&db, "GP5_SMOVE_SOURCE", 0));
  ASSERT_TRUE(members_match(&db, "GP5_SMOVE_SOURCE", {}));
  ASSERT_TRUE(size_match(&db, "GP5_SMOVE_DESTINATION", 3));
  ASSERT_TRUE(members_match(&db, "GP5_SMOVE_DESTINATION", {"a", "x", "l"}));


  // ***************** Group 6 Test *****************
  // source = {a, b, c, d}  (expire key);
  // destination = {a, c}
  // SMove source destination d
  // source = {}
  // destination = {d}
  std::vector<std::string> gp6_source {"a", "b", "c", "d"};
  std::vector<std::string> gp6_destination {"a", "c"};
  s = db.SAdd("GP6_SMOVE_SOURCE", gp6_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP6_SMOVE_DESTINATION", gp6_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  ASSERT_TRUE(make_expired(&db, "GP6_SMOVE_SOURCE"));

  s = db.SMove("GP6_SMOVE_SOURCE", "GP6_SMOVE_DESTINATION", "d", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  ASSERT_TRUE(size_match(&db, "GP6_SMOVE_SOURCE", 0));
  ASSERT_TRUE(members_match(&db, "GP6_SMOVE_SOURCE", {}));
  ASSERT_TRUE(size_match(&db, "GP6_SMOVE_DESTINATION", 2));
  ASSERT_TRUE(members_match(&db, "GP6_SMOVE_DESTINATION", {"a", "c"}));


  // ***************** Group 7 Test *****************
  // source = {a, b, c, d}
  // destination = {a, c}
  // SMove source destination x
  // source = {a, b, c, d}
  // destination = {a, c}
  std::vector<std::string> gp7_source {"a", "b", "c", "d"};
  std::vector<std::string> gp7_destination {"a", "c"};
  s = db.SAdd("GP7_SMOVE_SOURCE", gp7_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP7_SMOVE_DESTINATION", gp7_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.SMove("GP7_SMOVE_SOURCE", "GP7_SMOVE_DESTINATION", "x", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  ASSERT_TRUE(size_match(&db, "GP7_SMOVE_SOURCE", 4));
  ASSERT_TRUE(members_match(&db, "GP7_SMOVE_SOURCE", {"a", "b", "c", "d"}));
  ASSERT_TRUE(size_match(&db, "GP7_SMOVE_DESTINATION", 2));
  ASSERT_TRUE(members_match(&db, "GP7_SMOVE_DESTINATION", {"a", "c"}));


  // ***************** Group 8 Test *****************
  // source = {a, b, c, d}
  // destination = {a, c, d}
  // SMove source destination d
  // source = {a, b, c, d}
  // destination = {a, c, d}
  std::vector<std::string> gp8_source {"a", "b", "c", "d"};
  std::vector<std::string> gp8_destination {"a", "c", "d"};
  s = db.SAdd("GP8_SMOVE_SOURCE", gp8_source, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP8_SMOVE_DESTINATION", gp8_destination, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.SMove("GP8_SMOVE_SOURCE", "GP8_SMOVE_DESTINATION", "d", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  ASSERT_TRUE(size_match(&db, "GP8_SMOVE_SOURCE", 3));
  ASSERT_TRUE(members_match(&db, "GP8_SMOVE_SOURCE", {"a", "b", "c"}));
  ASSERT_TRUE(size_match(&db, "GP8_SMOVE_DESTINATION", 3));
  ASSERT_TRUE(members_match(&db, "GP8_SMOVE_DESTINATION", {"a", "c", "d"}));
}

// SRem
TEST_F(SetsTest, SRemTest) {
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

  ASSERT_TRUE(size_match(&db, "GP1_SREM_KEY", 2));
  ASSERT_TRUE(members_match(&db, "GP1_SREM_KEY", {"c", "d"}));


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

  ASSERT_TRUE(size_match(&db, "GP2_SREM_KEY", 4));
  ASSERT_TRUE(members_match(&db, "GP2_SREM_KEY", {"a", "b", "c", "d"}));


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

  ASSERT_TRUE(make_expired(&db, "GP4_SREM_KEY"));

  std::vector<std::string> gp4_del_members {"a", "b"};
  s = db.SRem("GP4_SREM_KEY", gp4_del_members, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);

  ASSERT_TRUE(size_match(&db, "GP4_SREM_KEY", 0));
  ASSERT_TRUE(members_match(&db, "GP4_SREM_KEY", {}));
}

// SUnion
TEST_F(SetsTest, SUnionTest) {
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SUNION key1 key2 key3  = {a, b, c, d, e}
  std::vector<std::string> gp1_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp1_members2 {"a", "c"};
  std::vector<std::string> gp1_members3 {"a", "c", "e"};
  s = db.SAdd("GP1_SUNION_KEY1", gp1_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SUNION_KEY2", gp1_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP1_SUNION_KEY3", gp1_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp1_keys {"GP1_SUNION_KEY1",
      "GP1_SUNION_KEY2", "GP1_SUNION_KEY3"};
  std::vector<std::string> gp1_members_out;
  s = db.SUnion(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp1_members_out, {"a", "b", "c", "d", "e"}));

  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}          (expire key);
  // SUNION key1 key2 key3  = {a, b, c, d}
  std::map<BlackWidow::DataType, rocksdb::Status> gp1_type_status;
  db.Expire("GP1_SUNION_KEY3", 1, &gp1_type_status);
  ASSERT_TRUE(gp1_type_status[BlackWidow::DataType::kSets].ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  gp1_members_out.clear();

  s = db.SUnion(gp1_keys, &gp1_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp1_members_out, {"a", "b", "c", "d"}));


  // ***************** Group 2 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SUNION key1 key2 key3 not_exist_key = {a, b, c, d, e}
  std::vector<std::string> gp2_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp2_members2 {"a", "c"};
  std::vector<std::string> gp2_members3 {"a", "c", "e"};
  s = db.SAdd("GP2_SUNION_KEY1", gp2_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP2_SUNION_KEY2", gp2_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP2_SUNION_KEY3", gp2_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp2_keys {"GP2_SUNION_KEY1",
      "GP2_SUNION_KEY2", "GP2_SUNION_KEY3", "GP2_NOT_EXIST_KEY"};
  std::vector<std::string> gp2_members_out;
  s = db.SUnion(gp2_keys, &gp2_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp2_members_out, {"a", "b", "c", "d", "e"}));


  // ***************** Group 3 Test *****************
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {}
  // SUNION key1 key2 key3 = {a, b, c, d}
  std::vector<std::string> gp3_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp3_members2 {"a", "c"};
  std::vector<std::string> gp3_members3 {"a", "c", "e", "f", "g"};
  s = db.SAdd("GP3_SUNION_KEY1", gp3_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP3_SUNION_KEY2", gp3_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP3_SUNION_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);

  s = db.SRem("GP3_SUNION_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);
  s = db.SCard("GP3_SUNION_KEY3", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);
  std::vector<std::string> gp3_members_out;
  s = db.SMembers("GP3_SUNION_KEY3", &gp3_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp3_members_out.size(), 0);

  std::vector<std::string> gp3_keys {"GP3_SUNION_KEY1",
      "GP3_SUNION_KEY2", "GP3_SUNION_KEY3"};
  gp3_members_out.clear();
  s = db.SUnion(gp3_keys, &gp3_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp3_members_out, {"a", "b", "c", "d"}));


  // ***************** Group 4 Test *****************
  // key1 = {a, b, c, d}
  // SUNION key1 = {a, b, c, d}
  std::vector<std::string> gp4_members1 {"a", "b", "c", "d"};
  s = db.SAdd("GP4_SUNION_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  std::vector<std::string> gp4_keys {"GP4_SUNION_KEY1"};
  std::vector<std::string> gp4_members_out;
  s = db.SUnion(gp4_keys, &gp4_members_out);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(members_match(gp4_members_out, {"a", "b", "c", "d"}));
}

// SUnionstore
TEST_F(SetsTest, SUnionstoreTest) {
  int32_t ret = 0;

  // ***************** Group 1 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SUNIONSTORE destination key1 key2 key3
  // destination = {a, b, c, d, e}
  std::vector<std::string> gp1_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp1_members2 {"a", "c"};
  std::vector<std::string> gp1_members3 {"a", "c", "e"};
  s = db.SAdd("GP1_SUNIONSTORE_KEY1", gp1_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP1_SUNIONSTORE_KEY2", gp1_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP1_SUNIONSTORE_KEY3", gp1_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp1_keys {"GP1_SUNIONSTORE_KEY1",
      "GP1_SUNIONSTORE_KEY2", "GP1_SUNIONSTORE_KEY3"};
  s = db.SUnionstore("GP1_SUNIONSTORE_DESTINATION1", gp1_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);
  ASSERT_TRUE(size_match(&db, "GP1_SUNIONSTORE_DESTINATION1", 5));
  ASSERT_TRUE(members_match(&db, "GP1_SUNIONSTORE_DESTINATION1",
              {"a", "b", "c", "d", "e"}));

  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}          (expire key);
  // SUNIONSTORE destination key1 key2 key3
  // destination = {a, b, c, d}
  ASSERT_TRUE(make_expired(&db, "GP1_SUNIONSTORE_KEY3"));

  s = db.SUnionstore("GP1_SUNIONSTORE_DESTINATION1", gp1_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  ASSERT_TRUE(size_match(&db, "GP1_SUNIONSTORE_DESTINATION1", 4));
  ASSERT_TRUE(members_match(&db, "GP1_SUNIONSTORE_DESTINATION1",
              {"a", "b", "c", "d"}));


  // ***************** Group 2 Test *****************
  // destination already exists, it is overwritten.
  // destination = {a, x, l}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {a, c, e}
  // SUNIONSTORE destination key1 key2 key3
  // destination = {a, b, c, d, e}
  std::vector<std::string> gp2_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp2_members2 {"a", "c"};
  std::vector<std::string> gp2_members3 {"a", "c", "e"};
  s = db.SAdd("GP2_SUNIONSTORE_KEY1", gp2_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP2_SUNIONSTORE_KEY2", gp2_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP2_SUNIONSTORE_KEY3", gp2_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp2_keys {"GP2_SUNIONSTORE_KEY1",
      "GP2_SUNIONSTORE_KEY2", "GP2_SUNIONSTORE_KEY3"};
  s = db.SUnionstore("GP2_SUNIONSTORE_DESTINATION1", gp2_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);
  ASSERT_TRUE(size_match(&db, "GP2_SUNIONSTORE_DESTINATION1", 5));
  ASSERT_TRUE(members_match(&db, "GP2_SUNIONSTORE_DESTINATION1",
              {"a", "b", "c", "d", "e"}));


  // ***************** Group 3 Test *****************
  // destination = {}
  // key1 = {a, b, c, d}
  // key2 = {a, c}
  // key3 = {}
  // SUNIONSTORE destination key1 key2 key3
  // destination = {a, b, c, d}
  std::vector<std::string> gp3_members1 {"a", "b", "c", "d"};
  std::vector<std::string> gp3_members2 {"a", "c"};
  std::vector<std::string> gp3_members3 {"a", "x", "l"};
  s = db.SAdd("GP3_SUNIONSTORE_KEY1", gp3_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  s = db.SAdd("GP3_SUNIONSTORE_KEY2", gp3_members2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
  s = db.SAdd("GP3_SUNIONSTORE_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.SRem("GP3_SUNIONSTORE_KEY3", gp3_members3, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP3_SUNIONSTORE_KEY3", 0));
  ASSERT_TRUE(members_match(&db, "GP3_SUNIONSTORE_KEY3", {}));

  std::vector<std::string> gp3_keys {"GP3_SUNIONSTORE_KEY1",
      "GP3_SUNIONSTORE_KEY2", "GP3_SUNIONSTORE_KEY3"};
  s = db.SUnionstore("GP3_SUNIONSTORE_DESTINATION1", gp3_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);
  ASSERT_TRUE(size_match(&db, "GP3_SUNIONSTORE_DESTINATION1", 4));
  ASSERT_TRUE(members_match(&db, "GP3_SUNIONSTORE_DESTINATION1",
              {"a", "b", "c", "d"}));


  // ***************** Group 4 Test *****************
  // destination = {}
  // key1 = {a, x, l}
  // SUNIONSTORE destination key1 not_exist_key
  // destination = {a, x, l}
  std::vector<std::string> gp4_members1 {"a", "x", "l"};
  s = db.SAdd("GP4_SUNIONSTORE_KEY1", gp4_members1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  std::vector<std::string> gp4_keys {"GP4_SUNIONSTORE_KEY1",
      "GP4_SUNIONSTORE_NOT_EXIST_KEY"};
  std::vector<std::string> gp4_members_out;
  s = db.SUnionstore("GP4_SUNIONSTORE_DESTINATION1", gp4_keys, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);
  ASSERT_TRUE(size_match(&db, "GP4_SUNIONSTORE_DESTINATION1", 3));
  ASSERT_TRUE(members_match(&db, "GP4_SUNIONSTORE_DESTINATION1",
              {"a", "x", "l"}));
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
