//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <thread>

#include "blackwidow/blackwidow.h"

int main() {
  blackwidow::Options options;
  options.create_if_missing = true;
  blackwidow::BlackWidow db;
  blackwidow::Status s = db.Open(options, "./db");
  if (s.ok()) {
    printf("Open success\n");
  } else {
    printf("Open failed, error: %s\n", s.ToString().c_str());
    return -1;
  }
  // HSet
  int32_t res;
  s = db.HSet("TEST_KEY1", "TEST_FIELD1", "TEST_VALUE1", &res);
  printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);
  s = db.HSet("TEST_KEY1", "TEST_FIELD2", "TEST_VALUE2", &res);
  printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);

  s = db.HSet("TEST_KEY2", "TEST_FIELD1", "TEST_VALUE1", &res);
  printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);
  s = db.HSet("TEST_KEY2", "TEST_FIELD2", "TEST_VALUE2", &res);
  printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);
  s = db.HSet("TEST_KEY2", "TEST_FIELD3", "TEST_VALUE3", &res);
  printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);

  // HGet
  std::string value;
  s = db.HGet("TEST_KEY1", "TEST_FIELD1", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY1", "TEST_FIELD2", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY1", "TEST_FIELD3", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY_NOT_EXIST", "TEST_FIELD", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());

  // Compact
  s = db.Compact();
  printf("Compact return: %s\n", s.ToString().c_str());

  // Expire
  s = db.Expire("TEST_KEY1", 1);
  printf("Expire return: %s\n", s.ToString().c_str());
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  s = db.HGet("TEST_KEY1", "TEST_FIELD1", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY1", "TEST_FIELD2", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());

  s = db.HGet("TEST_KEY2", "TEST_FIELD1", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY2", "TEST_FIELD2", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY2", "TEST_FIELD3", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());

  // Compact
  s = db.Compact();
  printf("Compact return: %s\n", s.ToString().c_str());

  s = db.HGet("TEST_KEY2", "TEST_FIELD1", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY2", "TEST_FIELD2", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());
  s = db.HGet("TEST_KEY2", "TEST_FIELD3", &value);
  printf("HGet return: %s, value = %s\n", s.ToString().c_str(), value.c_str());

  // Exists
  s = db.HSet("TEST_KEY1", "TEST_FIELD1", "TEST_VALUE1", &res);
  printf("HSet return: %s, res = %d\n", s.ToString().c_str(), res);
  s = db.HExists("TEST_KEY1", "TEST_FIELD1");
  printf("HExists return: %s\n", s.ToString().c_str());
  return 0;
}
