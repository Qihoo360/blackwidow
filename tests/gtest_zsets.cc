//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <thread>
#include <iostream>

#include "blackwidow/blackwidow.h"

using namespace blackwidow;

class ZSetsTest : public ::testing::Test {
 public:
  ZSetsTest() {
    std::string path = "./db/zsets";
    if (access(path.c_str(), F_OK)) {
      mkdir(path.c_str(), 0755);
    }
    options.create_if_missing = true;
    s = db.Open(options, path);
    if (!s.ok()) {
      printf("Open db failed, exit...\n");
      exit(1);
    }
  }
  virtual ~ZSetsTest() { }

  static void SetUpTestCase() { }
  static void TearDownTestCase() { }

  blackwidow::Options options;
  blackwidow::BlackWidow db;
  blackwidow::Status s;
};

static bool score_members_match(blackwidow::BlackWidow *const db,
                                const Slice& key,
                                const std::vector<BlackWidow::ScoreMember>& expect_sm) {
  std::vector<BlackWidow::ScoreMember> sm_out;
  Status s = db->ZRange(key, 0, -1, &sm_out);
  if (!s.ok() && !s.IsNotFound()) {
    return false;
  }
  if (sm_out.size() != expect_sm.size()) {
    return false;
  }
  if (s.IsNotFound() && expect_sm.empty()) {
    return true;
  }
  for (int idx = 0; idx < sm_out.size(); ++idx) {
    if (expect_sm[idx].score != sm_out[idx].score
      || expect_sm[idx].member != sm_out[idx].member) {
      return false;
    }
  }
  return true;
}

static bool score_members_match(const std::vector<BlackWidow::ScoreMember>& sm_out,
                                const std::vector<BlackWidow::ScoreMember>& expect_sm) {
  if (sm_out.size() != expect_sm.size()) {
    return false;
  }
  for (int idx = 0; idx < sm_out.size(); ++idx) {
    if (expect_sm[idx].score != sm_out[idx].score
      || expect_sm[idx].member != sm_out[idx].member) {
      return false;
    }
  }
  return true;
}

static bool size_match(blackwidow::BlackWidow *const db,
                       const Slice& key,
                       int32_t expect_size) {
  int32_t size = 0;
  Status s = db->ZCard(key, &size);
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
  if (!ret || !type_status[BlackWidow::DataType::kZSets].ok()) {
    return false;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  return true;
}

static bool delete_key(blackwidow::BlackWidow *const db,
                       const Slice& key) {
  std::vector<std::string> del_keys = {key.ToString()};
  std::map<BlackWidow::DataType, blackwidow::Status> type_status;
  db->Del(del_keys, &type_status);
  return type_status[BlackWidow::DataType::kZSets].ok();
}

// ZAdd
TEST_F(ZSetsTest, ZAddTest) {
  int32_t ret;

  // ***************** Group 1 Test *****************
  std::vector<BlackWidow::ScoreMember> gp1_sm {{3.23, "MM1"}, {0, "MM2"}, {8.0004, "MM3"}, {-0.54, "MM4"}};
  s = db.ZAdd("GP1_ZADD_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZADD_KEY", 4));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZADD_KEY", {{-0.54, "MM4"}, {0, "MM2"}, {3.23, "MM1"}, {8.0004, "MM3"}}));


  // ***************** Group 2 Test *****************
  std::vector<BlackWidow::ScoreMember> gp2_sm {{0, "MM1"}, {0, "MM1"}, {0, "MM2"}, {0, "MM3"}};
  s = db.ZAdd("GP2_ZADD_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZADD_KEY", {{0, "MM1"}, {0, "MM2"}, {0, "MM3"}}));


  // ***************** Group 3 Test *****************
  std::vector<BlackWidow::ScoreMember> gp3_sm {{1/1.0, "MM1"}, {1/3.0, "MM2"}, {1/6.0, "MM3"}, {1/7.0, "MM4"}};
  s = db.ZAdd("GP3_ZADD_KEY", gp3_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZADD_KEY", 4));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZADD_KEY", {{1/7.0, "MM4"}, {1/6.0, "MM3"}, {1/3.0, "MM2"}, {1/1.0, "MM1"}}));


  // ***************** Group 4 Test *****************
  std::vector<BlackWidow::ScoreMember> gp4_sm {{-1/1.0, "MM1"}, {-1/3.0, "MM2"}, {-1/6.0, "MM3"}, {-1/7.0, "MM4"}};
  s = db.ZAdd("GP4_ZADD_KEY", gp4_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);
  ASSERT_TRUE(size_match(&db, "GP4_ZADD_KEY", 4));
  ASSERT_TRUE(score_members_match(&db, "GP4_ZADD_KEY", {{-1/1.0, "MM1"}, {-1/3.0, "MM2"}, {-1/6.0, "MM3"}, {-1/7.0, "MM4"}}));


  // ***************** Group 5 Test *****************
  // [0, MM1]
  s = db.ZAdd("GP5_ZADD_KEY", {{0, "MM1"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY", {{0, "MM1"}}));

  // [-0.5333, MM2]
  // [0,       MM1]
  s = db.ZAdd("GP5_ZADD_KEY", {{-0.5333, "MM2"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(2, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 2));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY", {{-0.5333, "MM2"}, {0, "MM1"}}));

  // [-0.5333,      MM2]
  // [0,            MM1]
  // [1.79769e+308, MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{1.79769e+308, "MM3"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY",
        {{-0.5333, "MM2"}, {0, "MM1"}, {1.79769e+308, "MM3"}}));

  // [-0.5333,      MM2]
  // [0,            MM1]
  // [50000,        MM4]
  // [1.79769e+308, MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{50000, "MM4"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 4));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY",
        {{-0.5333, "MM2"}, {0, "MM1"}, {50000, "MM4"}, {1.79769e+308, "MM3"}}));

  // [-1.79769e+308, MM5]
  // [-0.5333,       MM2]
  // [0,             MM1]
  // [50000,         MM4]
  // [1.79769e+308,  MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{-1.79769e+308, "MM5"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(5, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 5));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY",
        {{-1.79769e+308, "MM5"}, {-0.5333,      "MM2"}, {0, "MM1"},
         {50000,         "MM4"}, {1.79769e+308, "MM3"}}));

  // [-1.79769e+308, MM5]
  // [-0.5333,       MM2]
  // [0,             MM1]
  // [0,             MM6]
  // [50000,         MM4]
  // [1.79769e+308,  MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{0, "MM6"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 6));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY",
        {{-1.79769e+308, "MM5"}, {-0.5333, "MM2"}, {0,            "MM1"},
         {0,             "MM6"}, {50000,   "MM4"}, {1.79769e+308, "MM3"}}));

  // [-1.79769e+308, MM5]
  // [-0.5333,       MM2]
  // [0,             MM1]
  // [50000,         MM4]
  // [100000,        MM6]
  // [1.79769e+308,  MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{100000, "MM6"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 6));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY",
        {{-1.79769e+308, "MM5"}, {-0.5333, "MM2"}, {0, "MM1"},
         {50000,         "MM4"}, {100000,  "MM6"}, {1.79769e+308, "MM3"}}));

  // [-1.79769e+308, MM5]
  // [-0.5333,       MM2]
  // [-0.5333,       MM7]
  // [0,             MM1]
  // [50000,         MM4]
  // [100000,        MM6]
  // [1.79769e+308,  MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{-0.5333, "MM7"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(7, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 7));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY",
        {{-1.79769e+308, "MM5"}, {-0.5333, "MM2"}, {-0.5333, "MM7"},
         {0,             "MM1"}, {50000,   "MM4"}, {100000,  "MM6"},
         {1.79769e+308,  "MM3"}}));

  // [-1.79769e+308, MM5]
  // [-0.5333,       MM2]
  // [-0.5333,       MM7]
  // [-1/3.0f,       MM8]
  // [0,             MM1]
  // [50000,         MM4]
  // [100000,        MM6]
  // [1.79769e+308,  MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{-1/3.0, "MM8"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(8, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 8));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY",
        {{-1.79769e+308, "MM5"}, {-0.5333,      "MM2"}, {-0.5333, "MM7"},
         {-1/3.0,       "MM8"},  {0,            "MM1"}, {50000,   "MM4"},
         {100000,        "MM6"}, {1.79769e+308, "MM3"}}));

  // [-1.79769e+308, MM5]
  // [-0.5333,       MM2]
  // [-0.5333,       MM7]
  // [-1/3.0f,       MM8]
  // [0,             MM1]
  // [1/3.0f,        MM9]
  // [50000,         MM4]
  // [100000,        MM6]
  // [1.79769e+308,  MM3]
  s = db.ZAdd("GP5_ZADD_KEY", {{1/3.0, "MM9"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 9));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY",
        {{-1.79769e+308, "MM5"}, {-0.5333, "MM2"}, {-0.5333,      "MM7"},
         {-1/3.0,        "MM8"}, {0,       "MM1"}, {1/3.0,        "MM9"},
         {50000,         "MM4"}, {100000,  "MM6"}, {1.79769e+308, "MM3"}}));

  // [0,  MM1]
  // [0,  MM2]
  // [0,  MM3]
  // [0,  MM4]
  // [0,  MM5]
  // [0,  MM6]
  // [0,  MM7]
  // [0,  MM8]
  // [0,  MM9]
  s = db.ZAdd("GP5_ZADD_KEY", {{0, "MM1"}, {0, "MM2"}, {0, "MM3"},
                               {0, "MM4"}, {0, "MM5"}, {0, "MM6"},
                               {0, "MM7"}, {0, "MM8"}, {0, "MM9"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP5_ZADD_KEY", 9));
  ASSERT_TRUE(score_members_match(&db, "GP5_ZADD_KEY",
        {{0, "MM1"}, {0, "MM2"}, {0, "MM3"},
         {0, "MM4"}, {0, "MM5"}, {0, "MM6"},
         {0, "MM7"}, {0, "MM8"}, {0, "MM9"}}));


  // ***************** Group 6 Test *****************
  std::vector<BlackWidow::ScoreMember> gp6_sm1 {{-1, "MM1"}, {0, "MM2"}, {1, "MM3"}};
  s = db.ZAdd("GP6_ZADD_KEY", gp6_sm1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP6_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP6_ZADD_KEY", {{-1, "MM1"}, {0, "MM2"}, {1, "MM3"}}));
  ASSERT_TRUE(make_expired(&db, "GP6_ZADD_KEY"));
  ASSERT_TRUE(size_match(&db, "GP6_ZADD_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP6_ZADD_KEY", {}));

  std::vector<BlackWidow::ScoreMember> gp6_sm2 {{-100, "MM1"}, {0, "MM2"}, {100, "MM3"}};
  s = db.ZAdd("GP6_ZADD_KEY", gp6_sm2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP6_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP6_ZADD_KEY", {{-100, "MM1"}, {0, "MM2"}, {100, "MM3"}}));


  // ***************** Group 7 Test *****************
  std::vector<BlackWidow::ScoreMember> gp7_sm1 {{-0.123456789, "MM1"}, {0, "MM2"}, {0.123456789, "MM3"}};
  s = db.ZAdd("GP7_ZADD_KEY", gp7_sm1, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP7_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP7_ZADD_KEY", {{-0.123456789, "MM1"}, {0, "MM2"}, {0.123456789, "MM3"}}));
  ASSERT_TRUE(delete_key(&db, "GP7_ZADD_KEY"));
  ASSERT_TRUE(size_match(&db, "GP7_ZADD_KEY", 0));
  ASSERT_TRUE(score_members_match(&db, "GP7_ZADD_KEY", {}));

  std::vector<BlackWidow::ScoreMember> gp7_sm2 {{-1234.56789, "MM1"}, {0, "MM2"}, {1234.56789, "MM3"}};
  s = db.ZAdd("GP7_ZADD_KEY", gp7_sm2, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP7_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP7_ZADD_KEY", {{-1234.56789, "MM1"}, {0, "MM2"}, {1234.56789, "MM3"}}));

  s = db.ZAdd("GP7_ZADD_KEY", {{1234.56789, "MM1"}}, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(size_match(&db, "GP7_ZADD_KEY", 3));
  ASSERT_TRUE(score_members_match(&db, "GP7_ZADD_KEY", {{0, "MM2"}, {1234.56789, "MM1"}, {1234.56789, "MM3"}}));
}

// ZSCORE
TEST_F(ZSetsTest, ZScoreTest) {
  int32_t ret;
  double score;

  // ***************** Group 1 Test *****************
  std::vector<BlackWidow::ScoreMember> gp1_sm {{54354.497895352, "MM1"}, {100.987654321,  "MM2"},
                                               {-100.000000001,  "MM3"}, {-100.000000002, "MM4"},
                                               {-100.000000001,  "MM5"}, {-100.000000002, "MM6"}};
  s = db.ZAdd("GP1_ZSCORE_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZSCORE_KEY", 6));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZSCORE_KEY", {{-100.000000002, "MM4"}, {-100.000000002, "MM6"},
                                                          {-100.000000001, "MM3"}, {-100.000000001, "MM5"},
                                                          {100.987654321,  "MM2"}, {54354.497895352,"MM1"}}));
  s = db.ZScore("GP1_ZSCORE_KEY", "MM1", &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(54354.497895352, score);

  s = db.ZScore("GP1_ZSCORE_KEY", "MM2", &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(100.987654321, score);

  s = db.ZScore("GP1_ZSCORE_KEY", "MM3", &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(-100.000000001, score);

  s = db.ZScore("GP1_ZSCORE_KEY", "MM4", &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(-100.000000002, score);

  s = db.ZScore("GP1_ZSCORE_KEY", "MM5", &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(-100.000000001, score);

  s = db.ZScore("GP1_ZSCORE_KEY", "MM6", &score);
  ASSERT_TRUE(s.ok());
  ASSERT_DOUBLE_EQ(-100.000000002, score);

  s = db.ZScore("GP1_ZSCORE_KEY", "MM7", &score);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_DOUBLE_EQ(0, score);


  // ***************** Group 2 Test *****************
  std::vector<BlackWidow::ScoreMember> gp2_sm {{4, "MM1"}, {3, "MM2"}, {2, "MM3"}, {1, "MM4"}};
  s = db.ZAdd("GP2_ZSCORE_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZSCORE_KEY", 4));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZSCORE_KEY", {{1,"MM4"}, {2,"MM3"}, {3, "MM2"}, {4, "MM1"}}));
  ASSERT_TRUE(make_expired(&db, "GP2_ZSCORE_KEY"));
  s = db.ZScore("GP2_ZSCORE_KEY", "MM1", &score);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_DOUBLE_EQ(0, score);


  // ***************** Group 3 Test *****************
  s = db.ZScore("GP3_ZSCORE_KEY", "MM1", &score);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_DOUBLE_EQ(0, score);
}

// ZCard
TEST_F(ZSetsTest, ZCardTest) {
  int32_t ret;
  double score;

  // ***************** Group 1 Test *****************
  std::vector<BlackWidow::ScoreMember> gp1_sm {{-1, "MM1"}, {-2, "MM2"}, {-3, "MM3"}, {-4, "MM4"}};
  s = db.ZAdd("GP1_ZCARD_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZCARD_KEY", 4));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZCARD_KEY", {{-4, "MM4"}, {-3, "MM3"}, {-2, "MM2"}, {-1, "MM1"}}));
  s = db.ZCard("GP1_ZCARD_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(4, ret);


  // ***************** Group 2 Test *****************
  std::vector<BlackWidow::ScoreMember> gp2_sm {{1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}};
  s = db.ZAdd("GP2_ZCARD_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(5, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZCARD_KEY", 5));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZCARD_KEY", {{1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));
  s = db.ZCard("GP2_ZCARD_KEY", &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(5, ret);


  // ***************** Group 3 Test *****************
  std::vector<BlackWidow::ScoreMember> gp3_sm {{1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}};
  s = db.ZAdd("GP3_ZCARD_KEY", gp3_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(5, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZCARD_KEY", 5));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZCARD_KEY", {{1, "MM1"}, {2, "MM2"}, {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));
  ASSERT_TRUE(make_expired(&db, "GP3_ZCARD_KEY"));
  s = db.ZCard("GP3_ZCARD_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, ret);


  // ***************** Group 4 Test *****************
  s = db.ZCard("GP4_ZCARD_KEY", &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, ret);
}

// ZRange
TEST_F(ZSetsTest, ZRangeTest) {
  int32_t ret;
  std::vector<BlackWidow::ScoreMember> score_members;

  // ***************** Group 1 Test *****************
  std::vector<BlackWidow::ScoreMember> gp1_sm {{0, "MM1"}};
  s = db.ZAdd("GP1_ZRANGE_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZRANGE_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZRANGE_KEY", {{0, "MM1"}}));

  s = db.ZRange("GP1_ZRANGE_KEY", 0, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM1"}}));


  // ***************** Group 2 Test *****************
  //
  // {0, MM0} {1, MM1} {2, MM2} {3, MM3} {4, MM4} {5, MM5} {6, MM6} {7, MM7} {8, MM8}
  //    0        1        2        3        4        5        6        7        8
  //   -9       -8       -7       -6       -5       -4       -3       -2       -1
  std::vector<BlackWidow::ScoreMember> gp2_sm {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                               {3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                               {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP2_ZRANGE_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZRANGE_KEY", 9));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZRANGE_KEY", {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                          {3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                          {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 0, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                  {3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                  {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -9, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                  {3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                  {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 0, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                  {3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                  {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -9, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                  {3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                  {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));


  s = db.ZRange("GP2_ZRANGE_KEY", -100, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                  {3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                  {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 0, 100, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                  {3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                  {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -100, 100, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                  {3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                  {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 0, 0, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -9, -9, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 8, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -1, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 0, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                  {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 0, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                  {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -9, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                  {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -9, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                  {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -100, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                  {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -100, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                  {3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 3, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -6, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 3, -4, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -6, 5, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 3, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                  {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -6, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                  {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 3, -1, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                  {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -6, 8, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                  {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", -6, 100, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                  {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZRange("GP2_ZRANGE_KEY", 3, 100, &score_members);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(score_members_match(score_members, {{3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                  {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));


  // ***************** Group 3 Test *****************
  std::vector<BlackWidow::ScoreMember> gp3_sm {{0, "MM1"}};
  s = db.ZAdd("GP3_ZRANGE_KEY", gp3_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(size_match(&db, "GP3_ZRANGE_KEY", 1));
  ASSERT_TRUE(score_members_match(&db, "GP3_ZRANGE_KEY", {{0, "MM1"}}));
  ASSERT_TRUE(make_expired(&db, "GP3_ZRANGE_KEY"));

  s = db.ZRange("GP3_ZRANGE_KEY", 0, -1, &score_members);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(score_members_match(score_members, {}));


  // ***************** Group 4 Test *****************
  s = db.ZRange("GP4_ZRANGE_KEY", 0, -1, &score_members);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(score_members_match(score_members, {}));
}

// ZCount
TEST_F(ZSetsTest, ZCountTest) {
  int32_t ret;

  // ***************** Group 1 Test *****************
  std::vector<BlackWidow::ScoreMember> gp1_sm {{101010.1010101, "MM1"}, {101010.0101010, "MM2"},
                                               {-100.000000001, "MM3"}, {-100.000000002, "MM4"},
                                               {-100.000000001, "MM5"}, {-100.000000002, "MM6"}};
  s = db.ZAdd("GP1_ZCOUNT_KEY", gp1_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(6, ret);
  ASSERT_TRUE(size_match(&db, "GP1_ZCOUNT_KEY", 6));
  ASSERT_TRUE(score_members_match(&db, "GP1_ZCOUNT_KEY", {{-100.000000002, "MM4"}, {-100.000000002, "MM6"},
                                                          {-100.000000001, "MM3"}, {-100.000000001, "MM5"},
                                                          {101010.0101010, "MM2"}, {101010.1010101, "MM1"}}));

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000002, 101010.1010101, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 6);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100000000, 100000000, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 6);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000002, -100.000000002, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000001, -100.000000001, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100000000, 100, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);

  s = db.ZCount("GP1_ZCOUNT_KEY", -100.000000001, 100000000, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 4);


  // ***************** Group 2 Test *****************
  std::vector<BlackWidow::ScoreMember> gp2_sm {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                               {3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                               {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP2_ZCOUNT_KEY", gp2_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP2_ZCOUNT_KEY", 9));
  ASSERT_TRUE(score_members_match(&db, "GP2_ZCOUNT_KEY", {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                          {3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                          {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));
  ASSERT_TRUE(make_expired(&db, "GP2_ZCOUNT_KEY"));
  s = db.ZCount("GP2_ZCOUNT_KEY", -100000000, 100000000, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);


  // ***************** Group 3 Test *****************
  s = db.ZCount("GP3_ZCOUNT_KEY", -100000000, 100000000, &ret);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(ret, 0);


  // ***************** Group 4 Test *****************
  std::vector<BlackWidow::ScoreMember> gp4_sm {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                               {3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                               {6, "MM6"}, {7, "MM7"}, {8, "MM8"}};
  s = db.ZAdd("GP4_ZCOUNT_KEY", gp4_sm, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(9, ret);
  ASSERT_TRUE(size_match(&db, "GP4_ZCOUNT_KEY", 9));
  ASSERT_TRUE(score_members_match(&db, "GP4_ZCOUNT_KEY", {{0, "MM0"}, {1, "MM1"}, {2, "MM2"},
                                                          {3, "MM3"}, {4, "MM4"}, {5, "MM5"},
                                                          {6, "MM6"}, {7, "MM7"}, {8, "MM8"}}));

  s = db.ZCount("GP4_ZCOUNT_KEY", -100, -50, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.ZCount("GP4_ZCOUNT_KEY", -100, 0, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZCount("GP4_ZCOUNT_KEY", -100, 4, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 5);

  s = db.ZCount("GP4_ZCOUNT_KEY", 0, 8, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 9);

  s = db.ZCount("GP4_ZCOUNT_KEY", 3, 5, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 3);

  s = db.ZCount("GP4_ZCOUNT_KEY", 100, 100, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 0);

  s = db.ZCount("GP4_ZCOUNT_KEY", 0, 0, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZCount("GP4_ZCOUNT_KEY", 8, 8, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 1);

  s = db.ZCount("GP4_ZCOUNT_KEY", 7, 8, &ret);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(ret, 2);
}
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
