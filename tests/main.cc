//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>
#include <thread>
#include <iostream>

#include "blackwidow/blackwidow.h"

using namespace blackwidow;

int main() {
  blackwidow::Options options;
  blackwidow::BlockBasedTableOptions table_options;
  blackwidow::BlackWidow db;
  blackwidow::Status s;

  std::string path = "./db";
  if (access(path.c_str(), F_OK)) {
    mkdir(path.c_str(), 0755);
  }
  options.create_if_missing = true;
  s = db.Open(options, table_options, path);
  uint64_t result = 0;
  s = db.GetUsage("all", &result);
  return 0;
}
