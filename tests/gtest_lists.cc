//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <thread>
#include <iostream>

#include "blackwidow/blackwidow.h"

using namespace blackwidow;

static bool elements_match(blackwidow::BlackWidow *const db,
                           const Slice& key,
                           const std::vector<std::string>& expect_elements) {
  std::vector<std::string> elements_out;
  Status s = db->LRange(key, 0, -1, &elements_out);
  if (!s.ok() && !s.IsNotFound()) {
    return false;
  }
  if (elements_out.size() != expect_elements.size()) {
    return false;
  }
  if (s.IsNotFound() && expect_elements.empty()) {
    return true;
  }
  for (uint64_t idx = 0; idx < elements_out.size(); ++idx) {
    if (strcmp(elements_out[idx].c_str(), expect_elements[idx].c_str())) {
      return false;
    }
  }
  return true;
}

static bool elements_match(const std::vector<std::string>& elements_out,
                           const std::vector<std::string>& expect_elements) {
  if (elements_out.size() != expect_elements.size()) {
    return false;
  }
  for (uint64_t idx = 0; idx < elements_out.size(); ++idx) {
    if (strcmp(elements_out[idx].c_str(), expect_elements[idx].c_str())) {
      return false;
    }
  }
  return true;
}

static bool len_match(blackwidow::BlackWidow *const db,
                      const Slice& key,
                      uint64_t expect_len) {
  uint64_t len = 0;
  Status s = db->LLen(key, &len);
  if (!s.ok() && !s.IsNotFound()) {
    return false;
  }
  if (s.IsNotFound() && !expect_len) {
    return true;
  }
  return len == expect_len;
}

static bool make_expired(blackwidow::BlackWidow *const db,
                         const Slice& key) {
  std::map<BlackWidow::DataType, rocksdb::Status> type_status;
  int ret = db->Expire(key, 1, &type_status);
  if (!ret || !type_status[BlackWidow::DataType::kLists].ok()) {
    return false;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  return true;
}

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

// LPush
TEST_F(ListsTest, LPushTest) {
  uint64_t num;
  std::string element;

  // ***************** Group 1 Test *****************
  //  "s" -> "l" -> "a" -> "s" -> "h"
  std::vector<std::string> gp1_nodes {"h", "s", "a", "l", "s"};
  s = db.LPush("GP1_LPUSH_KEY", gp1_nodes, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_nodes.size(), num);
  ASSERT_TRUE(len_match(&db, "GP1_LPUSH_KEY", gp1_nodes.size()));
  ASSERT_TRUE(elements_match(&db, "GP1_LPUSH_KEY", {"s", "l", "a", "s", "h"}));


  // ***************** Group 2 Test *****************
  //  "a" -> "x" -> "l"
  std::vector<std::string> gp2_nodes1 {"l", "x", "a"};
  s = db.LPush("GP2_LPUSH_KEY", gp2_nodes1, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_nodes1.size(), num);
  ASSERT_TRUE(len_match(&db, "GP2_LPUSH_KEY", gp2_nodes1.size()));
  ASSERT_TRUE(elements_match(&db, "GP2_LPUSH_KEY", {"a", "x", "l"}));

  // "r" -> "o" -> "s" -> "e"
  std::vector<std::string> gp2_nodes2 {"e", "s", "o", "r"};
  ASSERT_TRUE(make_expired(&db, "GP2_LPUSH_KEY"));
  s = db.LPush("GP2_LPUSH_KEY", gp2_nodes2, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_nodes2.size(), num);
  ASSERT_TRUE(len_match(&db, "GP2_LPUSH_KEY", gp2_nodes2.size()));
  ASSERT_TRUE(elements_match(&db, "GP2_LPUSH_KEY", {"r", "o", "s", "e"}));


  // ***************** Group 3 Test *****************
  //  "d" -> "a" -> "v" -> "i" -> "d"
  std::vector<std::string> gp3_nodes1 {"d", "i", "v", "a", "d"};
  s = db.LPush("GP3_LPUSH_KEY", gp3_nodes1, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp3_nodes1.size(), num);
  ASSERT_TRUE(len_match(&db, "GP3_LPUSH_KEY", gp3_nodes1.size()));
  ASSERT_TRUE(elements_match(&db, "GP3_LPUSH_KEY", {"d", "a", "v", "i", "d"}));

  // Delete the key
  std::vector<std::string> del_keys = {"GP3_LPUSH_KEY"};
  std::map<BlackWidow::DataType, blackwidow::Status> type_status;
  db.Del(del_keys, &type_status);
  ASSERT_TRUE(type_status[BlackWidow::DataType::kLists].ok());

  // "g" -> "i" -> "l" -> "m" -> "o" -> "u" -> "r"
  std::vector<std::string> gp3_nodes2 {"r", "u", "o", "m", "l", "i", "g"};
  s = db.LPush("GP3_LPUSH_KEY", gp3_nodes2, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp3_nodes2.size(), num);
  ASSERT_TRUE(len_match(&db, "GP3_LPUSH_KEY", gp3_nodes2.size()));
  ASSERT_TRUE(elements_match(&db, "GP3_LPUSH_KEY", {"g", "i", "l", "m", "o", "u", "r"}));


  // ***************** Group 4 Test *****************
  //  "b" -> "l" -> "u" -> "e"
  std::vector<std::string> gp4_nodes1 {"e", "u", "l", "b"};
  s = db.LPush("GP4_LPUSH_KEY", gp4_nodes1, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp4_nodes1.size(), num);
  ASSERT_TRUE(len_match(&db, "GP4_LPUSH_KEY", gp4_nodes1.size()));
  ASSERT_TRUE(elements_match(&db, "GP4_LPUSH_KEY", {"b", "l", "u", "e"}));

  // "t" -> "h" -> "e" -> " " -> "b" -> "l" -> "u" -> "e"
  std::vector<std::string> gp4_nodes2 {" ", "e", "h", "t"};
  s = db.LPush("GP4_LPUSH_KEY", gp4_nodes2, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(8, num);
  ASSERT_TRUE(len_match(&db, "GP4_LPUSH_KEY", 8));
  ASSERT_TRUE(elements_match(&db, "GP4_LPUSH_KEY", {"t", "h", "e", " ", "b", "l", "u", "e"}));
}

// RPush
TEST_F(ListsTest, RPushTest) {
  uint64_t num;
  std::string element;

  // ***************** Group 1 Test *****************
  //  "s" -> "l" -> "a" -> "s" -> "h"
  std::vector<std::string> gp1_nodes {"s", "l", "a", "s", "h"};
  s = db.RPush("GP1_RPUSH_KEY", gp1_nodes, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_nodes.size(), num);
  ASSERT_TRUE(len_match(&db, "GP1_RPUSH_KEY", gp1_nodes.size()));
  ASSERT_TRUE(elements_match(&db, "GP1_RPUSH_KEY", {"s", "l", "a", "s", "h"}));


  // ***************** Group 2 Test *****************
  //  "a" -> "x" -> "l"
  std::vector<std::string> gp2_nodes1 {"a", "x", "l"};
  s = db.RPush("GP2_RPUSH_KEY", gp2_nodes1, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_nodes1.size(), num);
  ASSERT_TRUE(len_match(&db, "GP2_RPUSH_KEY", gp2_nodes1.size()));
  ASSERT_TRUE(elements_match(&db, "GP2_RPUSH_KEY", {"a", "x", "l"}));

  // "r" -> "o" -> "s" -> "e"
  std::vector<std::string> gp2_nodes2 {"r", "o", "s", "e"};
  ASSERT_TRUE(make_expired(&db, "GP2_RPUSH_KEY"));
  s = db.RPush("GP2_RPUSH_KEY", gp2_nodes2, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_nodes2.size(), num);
  ASSERT_TRUE(len_match(&db, "GP2_RPUSH_KEY", gp2_nodes2.size()));
  ASSERT_TRUE(elements_match(&db, "GP2_RPUSH_KEY", {"r", "o", "s", "e"}));


  // ***************** Group 3 Test *****************
  //  "d" -> "a" -> "v" -> "i" -> "d"
  std::vector<std::string> gp3_nodes1 {"d", "a", "v", "i", "d"};
  s = db.RPush("GP3_RPUSH_KEY", gp3_nodes1, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp3_nodes1.size(), num);
  ASSERT_TRUE(len_match(&db, "GP3_RPUSH_KEY", gp3_nodes1.size()));
  ASSERT_TRUE(elements_match(&db, "GP3_RPUSH_KEY", {"d", "a", "v", "i", "d"}));

  // Delete the key
  std::vector<std::string> del_keys = {"GP3_RPUSH_KEY"};
  std::map<BlackWidow::DataType, blackwidow::Status> type_status;
  db.Del(del_keys, &type_status);
  ASSERT_TRUE(type_status[BlackWidow::DataType::kLists].ok());

  // "g" -> "i" -> "l" -> "m" -> "o" -> "u" -> "r"
  std::vector<std::string> gp3_nodes2 {"g", "i", "l", "m", "o", "u", "r"};
  s = db.RPush("GP3_RPUSH_KEY", gp3_nodes2, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp3_nodes2.size(), num);
  ASSERT_TRUE(len_match(&db, "GP3_RPUSH_KEY", gp3_nodes2.size()));
  ASSERT_TRUE(elements_match(&db, "GP3_RPUSH_KEY", {"g", "i", "l", "m", "o", "u", "r"}));


  // ***************** Group 4 Test *****************
  //  "t" -> "h" -> "e" -> " "
  std::vector<std::string> gp4_nodes1 {"t", "h", "e", " "};
  s = db.RPush("GP4_RPUSH_KEY", gp4_nodes1, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp4_nodes1.size(), num);
  ASSERT_TRUE(len_match(&db, "GP4_RPUSH_KEY", gp4_nodes1.size()));
  ASSERT_TRUE(elements_match(&db, "GP4_RPUSH_KEY", {"t", "h", "e", " "}));

  // "t" -> "h" -> "e" -> " " -> "b" -> "l" -> "u" -> "e"
  std::vector<std::string> gp4_nodes2 {"b", "l", "u", "e"};
  s = db.RPush("GP4_RPUSH_KEY", gp4_nodes2, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(8, num);
  ASSERT_TRUE(len_match(&db, "GP4_RPUSH_KEY", 8));
  ASSERT_TRUE(elements_match(&db, "GP4_RPUSH_KEY", {"t", "h", "e", " ", "b", "l", "u", "e"}));
}

// LRange
TEST_F(ListsTest, LRangeTest) {
  uint64_t num;
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

  s = db.LRange("LRANGE_KEY", -100, -1, &result);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(result.size(), 100);
  for (int32_t i = 0; i < 100; i++) {
    ASSERT_STREQ(result[i].c_str(), values[i].c_str());
  }
  result.clear();

  s = db.LRange("LRANGE_KEY", -100, 0, &result);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(result.size(), 1);
  ASSERT_STREQ(result[0].c_str(), values[0].c_str());
  result.clear();

  s = db.LRange("LRANGE_KEY", -1, 100, &result);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(result.size(), 1);
  ASSERT_STREQ(result[0].c_str(), values[99].c_str());
  result.clear();

  s = db.LRange("LRANGE_KEY", -50, -20, &result);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(result.size(), 31);
  for (int32_t i = 50, j = 0; i < 80 && j < result.size(); i++, j++) {
    ASSERT_STREQ(result[j].c_str(), values[i].c_str());
  }
  result.clear();

  s = db.LRange("LRANGE_KEY", 0, -1, &result);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(result.size(), 100);
  for (int32_t i = 0; i < 100; i++) {
    ASSERT_STREQ(result[i].c_str(), values[i].c_str());
  }
  result.clear();

  // TODO(shq)
}

// LTrim
TEST_F(ListsTest, LTrimTest) {
  uint64_t num;
  std::vector<std::string> values;
  for (int32_t i = 0; i < 100; i++) {
    values.push_back("LTRIM_VALUE"+ std::to_string(i));
  }
  s = db.RPush("LTRIM_KEY", values, &num);
  ASSERT_EQ(num, values.size());
  ASSERT_TRUE(s.ok());

  std::vector<std::string> result;
  s = db.LRange("LTRIM_KEY", 0, 100, &result);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(result.size(), 100);
  for (int32_t i = 0; i < 100; i++) {
    ASSERT_STREQ(result[i].c_str(), values[i].c_str());
  }
  result.clear();

  s = db.LTrim("LTRIM_KEY", 0, 50);
  ASSERT_TRUE(s.ok());
  s = db.LRange("LTRIM_KEY", 0, 50, &result);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(result.size(), 51);
  for (int32_t i = 0; i < 51; i++) {
    ASSERT_STREQ(result[i].c_str(), values[i].c_str());
  }
  result.clear();
}

// LLen
TEST_F(ListsTest, LLenTest) {
  uint64_t num;

  // ***************** Group 1 Test *****************
  // "l" -> "x" -> "a"
  std::vector<std::string> gp1_nodes {"a", "x", "l"};
  s = db.LPush("GP1_LLEN_KEY", gp1_nodes, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_nodes.size(), num);
  ASSERT_TRUE(len_match(&db, "GP1_LLEN_KEY", gp1_nodes.size()));

  // The key has timeout
  ASSERT_TRUE(make_expired(&db, "GP1_LLEN_KEY"));
  ASSERT_TRUE(len_match(&db, "GP1_LLEN_KEY", 0));


  // ***************** Group 1 Test *****************
  // "p" -> "e" -> "r" -> "g"
  std::vector<std::string> gp2_nodes {"g", "r", "e", "p"};
  s = db.LPush("GP2_LLEN_KEY", gp2_nodes, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_nodes.size(), num);
  ASSERT_TRUE(len_match(&db, "GP2_LLEN_KEY", gp2_nodes.size()));
  ASSERT_TRUE(elements_match(&db, "GP2_LLEN_KEY", {"p", "e", "r", "g"}));

  // Delete the key
  std::vector<std::string> del_keys = {"GP2_LLEN_KEY"};
  std::map<BlackWidow::DataType, blackwidow::Status> type_status;
  db.Del(del_keys, &type_status);
  ASSERT_TRUE(type_status[BlackWidow::DataType::kLists].ok());
  ASSERT_TRUE(len_match(&db, "GP2_LLEN_KEY", 0));
  ASSERT_TRUE(elements_match(&db, "GP2_LLEN_KEY", {}));
}

// LPop
TEST_F(ListsTest, LPopTest) {
  uint64_t num;
  std::string element;

  // ***************** Group 1 Test *****************
  //  "l" -> "x" -> "a"
  std::vector<std::string> gp1_nodes {"a", "x", "l"};
  s = db.LPush("GP1_LPOP_KEY", gp1_nodes, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_nodes.size(), num);
  ASSERT_TRUE(len_match(&db, "GP1_LPOP_KEY", gp1_nodes.size()));
  ASSERT_TRUE(elements_match(&db, "GP1_LPOP_KEY", {"l", "x", "a"}));

  // "x" -> "a"
  s = db.LPop("GP1_LPOP_KEY", &element);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(element, "l");
  ASSERT_TRUE(len_match(&db, "GP1_LPOP_KEY", 2));
  ASSERT_TRUE(elements_match(&db, "GP1_LPOP_KEY", {"x", "a"}));

  // after lpop two element, list will be empty
  s = db.LPop("GP1_LPOP_KEY", &element);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(element, "x");
  s = db.LPop("GP1_LPOP_KEY", &element);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(element, "a");
  ASSERT_TRUE(len_match(&db, "GP1_LPOP_KEY", 0));
  ASSERT_TRUE(elements_match(&db, "GP1_LPOP_KEY", {}));

  // lpop empty list
  s = db.LPop("GP1_LPOP_KEY", &element);
  ASSERT_TRUE(s.IsNotFound());


  // ***************** Group 2 Test *****************
  //  "p" -> "e" -> "r" -> "g"
  std::vector<std::string> gp2_nodes {"g", "r", "e", "p"};
  s = db.LPush("GP2_LPOP_KEY", gp2_nodes, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_nodes.size(), num);
  ASSERT_TRUE(len_match(&db, "GP2_LPOP_KEY", gp2_nodes.size()));
  ASSERT_TRUE(elements_match(&db, "GP2_LPOP_KEY", {"p", "e", "r", "g"}));

  ASSERT_TRUE(make_expired(&db, "GP2_LPOP_KEY"));
  s = db.LPop("GP2_LPOP_KEY", &element);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(len_match(&db, "GP2_LPOP_KEY", 0));
  ASSERT_TRUE(elements_match(&db, "GP2_LPOP_KEY", {}));


  // ***************** Group 3 Test *****************
  // "p" -> "o" -> "m" -> "e" -> "i" -> "i"
  std::vector<std::string> gp3_nodes {"i", "i", "e", "m", "o", "p"};
  s = db.LPush("GP3_LPOP_KEY", gp3_nodes, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp3_nodes.size(), num);
  ASSERT_TRUE(len_match(&db, "GP3_LPOP_KEY", gp3_nodes.size()));
  ASSERT_TRUE(elements_match(&db, "GP3_LPOP_KEY", {"p", "o", "m", "e", "i", "i"}));

  // Delete the key, then try lpop
  std::vector<std::string> del_keys = {"GP3_LPOP_KEY"};
  std::map<BlackWidow::DataType, blackwidow::Status> type_status;
  db.Del(del_keys, &type_status);
  ASSERT_TRUE(type_status[BlackWidow::DataType::kLists].ok());
  ASSERT_TRUE(len_match(&db, "GP3_LPOP_KEY", 0));
  ASSERT_TRUE(elements_match(&db, "GP3_LPOP_KEY", {}));

  s = db.LPop("GP3_LPOP_KEY", &element);
  ASSERT_TRUE(s.IsNotFound());
}

// RPop
TEST_F(ListsTest, RPopTest) {
  uint64_t num;
  std::string element;

  // ***************** Group 1 Test *****************
  //  "a" -> "x" -> "l"
  std::vector<std::string> gp1_nodes {"l", "x", "a"};
  s = db.LPush("GP1_RPOP_KEY", gp1_nodes, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_nodes.size(), num);
  ASSERT_TRUE(len_match(&db, "GP1_RPOP_KEY", gp1_nodes.size()));
  ASSERT_TRUE(elements_match(&db, "GP1_RPOP_KEY", {"a", "x", "l"}));

  // "a" -> "x"
  s = db.RPop("GP1_RPOP_KEY", &element);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(element, "l");
  ASSERT_TRUE(len_match(&db, "GP1_RPOP_KEY", 2));
  ASSERT_TRUE(elements_match(&db, "GP1_RPOP_KEY", {"a", "x"}));

  // After rpop two element, list will be empty
  s = db.RPop("GP1_RPOP_KEY", &element);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(element, "x");
  s = db.RPop("GP1_RPOP_KEY", &element);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(element, "a");
  ASSERT_TRUE(len_match(&db, "GP1_RPOP_KEY", 0));
  ASSERT_TRUE(elements_match(&db, "GP1_RPOP_KEY", {}));

  // lpop empty list
  s = db.LPop("GP1_RPOP_KEY", &element);
  ASSERT_TRUE(s.IsNotFound());


  // ***************** Group 2 Test *****************
  //  "g" -> "r" -> "e" -> "p"
  std::vector<std::string> gp2_nodes {"p", "e", "r", "g"};
  s = db.LPush("GP2_RPOP_KEY", gp2_nodes, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_nodes.size(), num);
  ASSERT_TRUE(len_match(&db, "GP2_RPOP_KEY", gp2_nodes.size()));
  ASSERT_TRUE(elements_match(&db, "GP2_RPOP_KEY", {"g", "r", "e", "p"}));

  ASSERT_TRUE(make_expired(&db, "GP2_RPOP_KEY"));
  s = db.LPop("GP2_RPOP_KEY", &element);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(len_match(&db, "GP2_RPOP_KEY", 0));
  ASSERT_TRUE(elements_match(&db, "GP2_RPOP_KEY", {}));


  // ***************** Group 3 Test *****************
  // "p" -> "o" -> "m" -> "e" -> "i" -> "i"
  std::vector<std::string> gp3_nodes {"i", "i", "e", "m", "o", "p"};
  s = db.LPush("GP3_RPOP_KEY", gp3_nodes, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp3_nodes.size(), num);
  ASSERT_TRUE(len_match(&db, "GP3_RPOP_KEY", gp3_nodes.size()));
  ASSERT_TRUE(elements_match(&db, "GP3_RPOP_KEY", {"p", "o", "m", "e", "i", "i"}));

  // Delete the key, then try lpop
  std::vector<std::string> del_keys = {"GP3_RPOP_KEY"};
  std::map<BlackWidow::DataType, blackwidow::Status> type_status;
  db.Del(del_keys, &type_status);
  ASSERT_TRUE(type_status[BlackWidow::DataType::kLists].ok());
  ASSERT_TRUE(len_match(&db, "GP3_RPOP_KEY", 0));
  ASSERT_TRUE(elements_match(&db, "GP3_RPOP_KEY", {}));

  s = db.RPop("GP3_LPOP_KEY", &element);
  ASSERT_TRUE(s.IsNotFound());
}

// LIndex
TEST_F(ListsTest, RIndexTest) {
  uint64_t num;
  std::string element;

  // ***************** Group 1 Test *****************
  //  "z" -> "e" -> "p" -> "p" -> "l" -> "i" -> "n"
  //   0      1      2      3      4      5      6
  //  -7     -6     -5     -4     -3     -2     -1
  std::vector<std::string> gp1_nodes {"n", "i", "l", "p", "p", "e", "z"};
  s = db.LPush("GP1_LINDEX_KEY", gp1_nodes, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp1_nodes.size(), num);
  ASSERT_TRUE(len_match(&db, "GP1_LINDEX_KEY", gp1_nodes.size()));
  ASSERT_TRUE(elements_match(&db, "GP1_LINDEX_KEY", {"z", "e", "p", "p", "l", "i", "n"}));

  s = db.LIndex("GP1_LINDEX_KEY", 0, &element);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(element, "z");

  s = db.LIndex("GP1_LINDEX_KEY", 4, &element);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(element, "l");

  s = db.LIndex("GP1_LINDEX_KEY", 6, &element);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(element, "n");

  s = db.LIndex("GP1_LINDEX_KEY", 10, &element);
  ASSERT_TRUE(s.IsNotFound());

  s = db.LIndex("GP1_LINDEX_KEY", -1, &element);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(element, "n");

  s = db.LIndex("GP1_LINDEX_KEY", -4, &element);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(element, "p");

  s = db.LIndex("GP1_LINDEX_KEY", -7, &element);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(element, "z");

  s = db.LIndex("GP1_LINDEX_KEY", -10000, &element);
  ASSERT_TRUE(s.IsNotFound());


  // ***************** Group 2 Test *****************
  //  "b" -> "a" -> "t" -> "t" -> "l" -> "e"
  //   0      1      2      3      4      5
  //  -6     -5     -4     -3     -2     -1
  //  LIndex time out list
  std::vector<std::string> gp2_nodes {"b", "a", "t", "t", "l", "e"};
  s = db.RPush("GP2_LINDEX_KEY", gp2_nodes, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp2_nodes.size(), num);
  ASSERT_TRUE(len_match(&db, "GP2_LINDEX_KEY", gp2_nodes.size()));
  ASSERT_TRUE(elements_match(&db, "GP2_LINDEX_KEY", {"b", "a", "t", "t", "l", "e"}));

  ASSERT_TRUE(make_expired(&db, "GP2_LINDEX_KEY"));
  ASSERT_TRUE(len_match(&db, "GP2_LINDEX_KEY", 0));
  ASSERT_TRUE(elements_match(&db, "GP2_LINDEX_KEY", {}));
  s = db.LIndex("GP2_LINDEX_KEY", 0, &element);
  ASSERT_TRUE(s.IsNotFound());


  // ***************** Group 3 Test *****************
  //  "m" -> "i" -> "s" -> "t" -> "y"
  //   0      1      2      3      4
  //  -5     -4     -3     -2     -1
  //  LIndex the key that has been deleted
  std::vector<std::string> gp3_nodes {"m", "i", "s", "t", "y"};
  s = db.RPush("GP3_LINDEX_KEY", gp3_nodes, &num);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(gp3_nodes.size(), num);
  ASSERT_TRUE(len_match(&db, "GP3_LINDEX_KEY", gp3_nodes.size()));
  ASSERT_TRUE(elements_match(&db, "GP3_LINDEX_KEY", {"m", "i", "s", "t", "y"}));

  std::vector<std::string> del_keys = {"GP3_LINDEX_KEY"};
  std::map<BlackWidow::DataType, blackwidow::Status> type_status;
  db.Del(del_keys, &type_status);
  ASSERT_TRUE(type_status[BlackWidow::DataType::kLists].ok());
  ASSERT_TRUE(len_match(&db, "GP3_LINDEX_KEY", 0));
  ASSERT_TRUE(elements_match(&db, "GP3_LINDEX_KEY", {}));

  s = db.LIndex("GP3_LINDEX_KEY", 0, &element);
  ASSERT_TRUE(s.IsNotFound());


  // ***************** Group 4 Test *****************
  //  LIndex not exist key
  s = db.LIndex("GP4_LINDEX_KEY", 0, &element);
  ASSERT_TRUE(s.IsNotFound());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
