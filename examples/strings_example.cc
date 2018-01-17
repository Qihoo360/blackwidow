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
  // Set
  s = db.Set("TEST_KEY", "TEST_VALUE");
  printf("Set return: %s\n", s.ToString().c_str());

  // Get
  std::string value;
  s = db.Get("TEST_KEY", &value);
  printf("Get return: %s, value: %s\n", s.ToString().c_str(), value.c_str());

  // Setnx
  int32_t ret;
  s = db.Setnx("TEST_KEY", "TEST_VALUE", &ret);
  printf("Setnx return: %s, value: %s, ret: %d\n", s.ToString().c_str(), value.c_str(), ret);

  // Append
  std::string append_value;
  s = db.Append("TEST_KEY", "APPEND_VALUE", &ret);
  s = db.Get("TEST_KEY", &append_value);
  printf("Append return: %s, value: %s, ret: %d\n", s.ToString().c_str(), append_value.c_str(), ret);

  // Expire
  s = db.Expire("TEST_KEY", 1);
  printf("Expire return: %s\n", s.ToString().c_str());
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  s = db.Get("TEST_KEY", &value);
  printf("Get return: %s, value: %s\n", s.ToString().c_str(), value.c_str());

  // Compact
  s = db.Compact();
  printf("Compact return: %s\n", s.ToString().c_str());

  s = db.Setex("TEST_KEY", "TEST_VALUE", 1);
  printf("Setex return: %s\n", s.ToString().c_str());
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  s = db.Get("TEST_KEY", &value);
  printf("Get return: %s, value: %s\n", s.ToString().c_str(), value.c_str());

  s = db.Set("TEST_KEY", "TEST_VALUE");
  int32_t len = 0;
  s = db.Strlen("TEST_KEY", &len);
  printf("Strlen return: %s, strlen: %d\n", s.ToString().c_str(), len);

  return 0;
}
